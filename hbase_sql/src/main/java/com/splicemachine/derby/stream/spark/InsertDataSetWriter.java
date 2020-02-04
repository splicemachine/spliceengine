/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.utils.BulkLoadUtils;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import scala.Tuple2;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class InsertDataSetWriter<K,V> implements DataSetWriter{
    private static final Logger LOG=Logger.getLogger(InsertDataSetWriter.class);
    private int[] updateCounts;
    private DataSet dataSet;
    private JavaPairRDD<K, V> rdd;
    private OperationContext<? extends SpliceOperation> opContext;
    private Configuration config;
    private int[] pkCols;
    private String tableVersion;
    private ExecRow execRowDefinition;
    private RowLocation[] autoIncRowArray;
    private SpliceSequence[] sequences;
    private long heapConglom;
    private boolean isUpsert;
    private double sampleFraction;
    private TxnView txn;

    public InsertDataSetWriter(){
    }

    public InsertDataSetWriter(DataSet dataSet,
                               OperationContext<? extends SpliceOperation> opContext,
                               Configuration config,
                               int[] pkCols,
                               String tableVersion,
                               ExecRow execRowDefinition,
                               RowLocation[] autoIncRowArray,
                               SpliceSequence[] sequences,
                               long heapConglom,
                               boolean isUpsert,
                               double sampleFraction,
                               int[] updateCounts) throws StandardException {
        this.dataSet = dataSet;
        this.rdd=((SparkPairDataSet) dataSet.index(new EmptySparkPairDataSet<>())).wrapExceptions();
        this.opContext=opContext;
        this.config=config;
        this.pkCols=pkCols;
        this.tableVersion=tableVersion;
        this.execRowDefinition=execRowDefinition;
        this.autoIncRowArray=autoIncRowArray;
        this.sequences=sequences;
        this.heapConglom=heapConglom;
        this.isUpsert=isUpsert;
        this.sampleFraction = sampleFraction;
        this.updateCounts = updateCounts;
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException{

        if (sampleFraction > 0) {
            sampleAndSplitTable();
        }
        rdd.saveAsNewAPIHadoopDataset(config);
        if(opContext.getOperation()!=null){
            DMLWriteOperation writeOp = (DMLWriteOperation)opContext.getOperation();
            if (writeOp != null)
                writeOp.finalizeNestedTransaction();
            opContext.getOperation().fireAfterStatementTriggers();
        }
        ValueRow valueRow=new ValueRow(3);
        valueRow.setColumn(1,new SQLLongint(opContext.getRecordsWritten()));
        valueRow.setColumn(2,new SQLLongint());
        valueRow.setColumn(3,new SQLVarchar());
        InsertOperation insertOperation=((InsertOperation)opContext.getOperation());
        if(insertOperation!=null && opContext.isPermissive()) {
            long numBadRecords = opContext.getBadRecords();
            valueRow.setColumn(2,new SQLLongint(numBadRecords));
            if (numBadRecords > 0) {
                String fileName = opContext.getStatusDirectory();
                valueRow.setColumn(3,new SQLVarchar(fileName));
                if (insertOperation.isAboveFailThreshold(numBadRecords)) {
                    throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                }
            }
        } else if (updateCounts != null) {
            int total = 0;
            List<ExecRow> rows = new ArrayList<>();
            for (int count : updateCounts) {
                total += count;
                valueRow = new ValueRow(1);
                valueRow.setColumn(1, new SQLLongint(count));
                rows.add(valueRow);
            }
            assert total == opContext.getRecordsWritten();
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(rows, 1));
        }
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return opContext.getTxn();
        else
            return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }


    private void sampleAndSplitTable () throws StandardException {
        // collect index information for the table
        Activation activation = opContext.getActivation();
        DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
        ConglomerateDescriptor cd = dd.getConglomerateDescriptor(heapConglom);
        TableDescriptor td = dd.getTableDescriptor(cd.getTableID());
        ConglomerateDescriptorList list = td.getConglomerateDescriptorList();
        List<Long> allCongloms = Lists.newArrayList();
        allCongloms.add(td.getHeapConglomerateId());
        ArrayList<DDLMessage.TentativeIndex> tentativeIndexList = new ArrayList();
        DDLDriver ddlDriver=DDLDriver.driver();
        for(DDLMessage.DDLChange ddlChange : ddlDriver.ddlWatcher().getTentativeDDLs()){
            DDLMessage.TentativeIndex tentativeIndex = ddlChange.getTentativeIndex();
            if (tentativeIndex != null) {
                long table = tentativeIndex.getTable().getConglomerate();
                TxnView indexTxn = DDLUtils.getLazyTransaction(ddlChange.getTxnId());
                if (table == td.getHeapConglomerateId() && indexTxn.getCommitTimestamp() > 0 &&
                        indexTxn.getCommitTimestamp() < getTxn().getTxnId()) {
                    tentativeIndexList.add(tentativeIndex);
                    allCongloms.add(tentativeIndex.getIndex().getConglomerate());
                }
            }
        }

        for (ConglomerateDescriptor searchCD :list) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(getTxn().getTxnId(),
                        activation.getLanguageConnectionContext(),
                        td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                        td, searchCD.getIndexDescriptor(),td.getDefaultValue(searchCD.getIndexDescriptor().baseColumnPositions()[0]));
                tentativeIndexList.add(ddlChange.getTentativeIndex());
                allCongloms.add(searchCD.getConglomerateNumber());
            }
        }
        List<Tuple2<Long, byte[][]>> cutPoints = null;
        DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);

        // encode key/vale pairs for table and indexes
        RowAndIndexGenerator rowAndIndexGenerator =
                new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRowDefinition, autoIncRowArray,
                        sequences, heapConglom, getTxn(), opContext, tentativeIndexList);
        DataSet sampleRowAndIndexes = sampledDataSet.flatMap(rowAndIndexGenerator);

        // collect statistics for encoded key/value, include size and histogram
        RowKeyStatisticsFunction statisticsFunction =
                new RowKeyStatisticsFunction(td.getHeapConglomerateId(), tentativeIndexList);
        DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(statisticsFunction);

        List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> result = keyStatistics.collect();

        // Calculate cut points for main table and index tables
        cutPoints = BulkLoadUtils.getCutPoints(sampleFraction, result);

        // split table and indexes using the calculated cutpoints
        if (cutPoints != null && !cutPoints.isEmpty()) {
            SpliceLogUtils.info(LOG, "Split table %s and its indexes", heapConglom);
            BulkLoadUtils.splitTables(cutPoints);
        }
    }
}
