/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.utils.BulkLoadUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.*;
import java.util.*;

public class BulkInsertDataSetWriter extends BulkDataSetWriter implements DataSetWriter {

    private static final Logger LOG=Logger.getLogger(BulkInsertDataSetWriter.class);

    private String tableVersion;
    private int[] pkCols;
    private RowLocation[] autoIncrementRowLocationArray;
    private ExecRow execRow;
    private SpliceSequence[] spliceSequences;
    private String bulkImportDirectory;
    private boolean samplingOnly;
    private boolean outputKeysOnly;
    private boolean skipSampling;
    private String indexName;
    private double sampleFraction;

    public BulkInsertDataSetWriter(){
    }

    public BulkInsertDataSetWriter(DataSet dataSet,
                                   String tableVersion,
                                   int[] pkCols,
                                   RowLocation[] autoIncrementRowLocationArray,
                                   long heapConglom,
                                   ExecRow execRow,
                                   SpliceSequence[] spliceSequences,
                                   OperationContext operationContext,
                                   TxnView txn,
                                   String bulkImportDirectory,
                                   boolean samplingOnly,
                                   boolean outputKeysOnly,
                                   boolean skipSampling,
                                   String indexName,
                                   double sampleFraction,
                                   byte[] token) {
        super(dataSet, operationContext, heapConglom, txn, token);
        this.tableVersion = tableVersion;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.pkCols = pkCols;
        this.execRow = execRow;
        this.spliceSequences = spliceSequences;
        this.bulkImportDirectory = bulkImportDirectory;
        this.samplingOnly = samplingOnly;
        this.outputKeysOnly = outputKeysOnly;
        this.skipSampling = skipSampling;
        this.indexName = indexName;
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return operationContext.getTxn();
        else
            return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }

    /**
     *  1) Sample data to calculate key/value size and histogram for keys
     *  2) Calculate cut points and split table and indexes
     *  3) Read and encode data for table and indexes, hash to the partition where its rowkey falls into
     *  4) Sort keys in each partition and write to HFiles
     *  5) Load HFiles to HBase
     * @return
     * @throws StandardException
     */
    @Override
    public DataSet<ExecRow> write() throws StandardException {

        // collect index information for the table
        Activation activation = operationContext.getActivation();
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
                    indexTxn.getCommitTimestamp() < txn.getTxnId()) {
                    tentativeIndexList.add(tentativeIndex);
                    allCongloms.add(tentativeIndex.getIndex().getConglomerate());
                }
            }
        }

        if (outputKeysOnly) {
            // This option is for calculating split row keys in HBase format, provided that split row keys are
            // stored in a csv file
            long indexConglom = -1;
            boolean isUnique = false;
            for (ConglomerateDescriptor searchCD :list) {
                if (searchCD.isIndex() && !searchCD.isPrimaryKey() && indexName != null &&
                        searchCD.getObjectName().compareToIgnoreCase(indexName) == 0) {
                    indexConglom = searchCD.getConglomerateNumber();
                    DataValueDescriptor dvd =
                            td.getColumnDescriptor(
                                    searchCD.getIndexDescriptor().baseColumnPositions()[0]).getDefaultValue();
                    DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                            activation.getLanguageConnectionContext(),
                            td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                            td, searchCD.getIndexDescriptor(),td.getDefaultValue(searchCD.getIndexDescriptor().baseColumnPositions()[0]));
                    isUnique = searchCD.getIndexDescriptor().isUnique();
                    tentativeIndexList.add(ddlChange.getTentativeIndex());
                    allCongloms.add(searchCD.getConglomerateNumber());
                    break;
                }
            }
            RowAndIndexGenerator rowAndIndexGenerator =
                    new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow, autoIncrementRowLocationArray,
                            spliceSequences, heapConglom, txn, operationContext, tentativeIndexList);
            DataSet rowAndIndexes = dataSet.flatMap(rowAndIndexGenerator);
            DataSet keys = rowAndIndexes.mapPartitions(
                    new RowKeyGenerator(bulkImportDirectory, heapConglom, indexConglom, isUnique));
            List<String> files = keys.collect();
        }
        else {
            for (ConglomerateDescriptor searchCD :list) {
                if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                    DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                            activation.getLanguageConnectionContext(),
                            td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                            td, searchCD.getIndexDescriptor(),td.getDefaultValue(searchCD.getIndexDescriptor().baseColumnPositions()[0]));
                    tentativeIndexList.add(ddlChange.getTentativeIndex());
                    allCongloms.add(searchCD.getConglomerateNumber());
                }
            }
            List<Tuple2<Long, byte[][]>> cutPoints = null;
            if (!skipSampling) {
                if (sampleFraction == 0) {
                    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
                    sampleFraction = BulkLoadUtils.getSampleFraction(lcc);
                }
                DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);

                // encode key/vale pairs for table and indexes
                RowAndIndexGenerator rowAndIndexGenerator =
                        new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow, autoIncrementRowLocationArray,
                                spliceSequences, heapConglom, txn, operationContext, tentativeIndexList);
                DataSet sampleRowAndIndexes = sampledDataSet.flatMap(rowAndIndexGenerator);

                // collect statistics for encoded key/value, include size and histgram
                RowKeyStatisticsFunction statisticsFunction =
                        new RowKeyStatisticsFunction(td.getHeapConglomerateId(), tentativeIndexList);
                DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(statisticsFunction);

                List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> result = keyStatistics.collect();

                // Calculate cut points for main table and index tables
                cutPoints = BulkLoadUtils.getCutPoints(sampleFraction, result);

                // dump cut points to file system for reference
                ImportUtils.dumpCutPoints(cutPoints, bulkImportDirectory);
                operationContext.reset();
            }
            if (!samplingOnly && !outputKeysOnly) {

                // split table and indexes using the calculated cutpoints
                if (cutPoints != null && !cutPoints.isEmpty()) {
                    BulkLoadUtils.splitTables(cutPoints);
                }

                // get the actual start/end key for each partition after split
                final List<BulkImportPartition> bulkImportPartitions =
                        getBulkImportPartitions(allCongloms, bulkImportDirectory);

                RowAndIndexGenerator rowAndIndexGenerator = new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow,
                        autoIncrementRowLocationArray, spliceSequences, heapConglom, txn,
                        operationContext, tentativeIndexList);

                String compressionAlgorithm = HConfiguration.getConfiguration().getCompressionAlgorithm();

                // Write to HFile
                HFileGenerationFunction hfileGenerationFunction =
                        new BulkInsertHFileGenerationFunction(operationContext, txn.getTxnId(),
                                heapConglom, compressionAlgorithm, bulkImportPartitions, pkCols, tableVersion, tentativeIndexList);

                DataSet rowAndIndexes = dataSet.flatMap(rowAndIndexGenerator);
                assert rowAndIndexes instanceof SparkDataSet;

                partitionUsingRDDSortUsingDataFrame(bulkImportPartitions, rowAndIndexes, hfileGenerationFunction);
                Map<Long, Boolean> granted = new HashMap<>();
                try {
                    if (token != null && token.length > 0) {
                        granted = grantCreatePrivilege(allCongloms);
                    }
                    bulkLoad(bulkImportPartitions, bulkImportDirectory, "Insert:");
                } finally {

                    if (granted.size() > 0) {
                        revokeCreatePrivilege(granted);
                    }
                }
            }
        }
        ValueRow valueRow = new ValueRow(3);
        valueRow.setColumn(1, new SQLLongint(operationContext.getRecordsWritten()));
        valueRow.setColumn(2, new SQLLongint());
        valueRow.setColumn(3, new SQLVarchar());
        InsertOperation insertOperation = ((InsertOperation) operationContext.getOperation());
        if (insertOperation != null && operationContext.isPermissive()) {
            long numBadRecords = operationContext.getBadRecords();
            valueRow.setColumn(2, new SQLLongint(numBadRecords));
            if (numBadRecords > 0) {
                String fileName = operationContext.getStatusDirectory();
                valueRow.setColumn(3, new SQLVarchar(fileName));
                if (insertOperation.isAboveFailThreshold(numBadRecords)) {
                    throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                }
            }
        }

        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    private Map<Long, Boolean> grantCreatePrivilege(List<Long> congloms) throws StandardException {
        try {
            Map<Long, Boolean> result = new HashMap<>();
            SpliceLogUtils.info(LOG, "granting necessary privileges for bulk load");
            PartitionFactory tableFactory = SIDriver.driver().getTableFactory();
            for (Long conglomId : congloms) {
                Partition partition = tableFactory.getTable(Long.toString(conglomId));
                Boolean granted = partition.grantCreatePrivilege();
                result.put(conglomId, granted);
            }
            return result;
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private void revokeCreatePrivilege(Map<Long, Boolean> granted) throws StandardException{
        try {
            PartitionFactory tableFactory = SIDriver.driver().getTableFactory();
            for (Long conglomId : granted.keySet()) {
                if (granted.get(conglomId)) {
                    Partition partition = tableFactory.getTable(Long.toString(conglomId));
                    partition.revokeCreatePrivilege();
                }
            }
        }
        catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
