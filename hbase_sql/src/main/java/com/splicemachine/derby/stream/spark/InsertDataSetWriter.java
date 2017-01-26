/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.util.Collections;

import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.db.iapi.types.SQLLongint;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class InsertDataSetWriter<K,V> implements DataSetWriter{
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
    private TxnView txn;

    public InsertDataSetWriter(){
    }

    public InsertDataSetWriter(JavaPairRDD<K, V> rdd,
                               OperationContext<? extends SpliceOperation> opContext,
                               Configuration config,
                               int[] pkCols,
                               String tableVersion,
                               ExecRow execRowDefinition,
                               RowLocation[] autoIncRowArray,
                               SpliceSequence[] sequences,
                               long heapConglom,
                               boolean isUpsert){
        this.rdd=rdd;
        this.opContext=opContext;
        this.config=config;
        this.pkCols=pkCols;
        this.tableVersion=tableVersion;
        this.execRowDefinition=execRowDefinition;
        this.autoIncRowArray=autoIncRowArray;
        this.sequences=sequences;
        this.heapConglom=heapConglom;
        this.isUpsert=isUpsert;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
            rdd.saveAsNewAPIHadoopDataset(config);
            if(opContext.getOperation()!=null){
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
                    String fileName = opContext.getBadRecordFileName();
                    valueRow.setColumn(3,new SQLVarchar(fileName));
                    if (insertOperation.isAboveFailThreshold(numBadRecords)) {
                        throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                    }
                }
            }
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(new LocatedRow(valueRow)), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new InsertPipelineWriter(pkCols,tableVersion,execRowDefinition,autoIncRowArray,sequences,heapConglom,
                txn,opContext,isUpsert);
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

}
