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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class DeleteDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K,Either<Exception, V>> rdd;
    private final OperationContext operationContext;
    private final Configuration conf;
    private final int[] updateCounts;
    private TxnView txnView;

    public DeleteDataSetWriter(JavaPairRDD<K, Either<Exception, V>> rdd, OperationContext operationContext, Configuration conf, int[] updateCounts){
        this.rdd=rdd;
        this.operationContext=operationContext;
        this.conf=conf;
        this.updateCounts=updateCounts;
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf);
        if (operationContext.getOperation() != null) {
            DMLWriteOperation writeOp = (DMLWriteOperation)operationContext.getOperation();
            if (writeOp != null)
                writeOp.finalizeNestedTransaction();
            operationContext.getOperation().fireAfterStatementTriggers();
        }
        if (updateCounts != null) {
            int total = 0;
            List<ExecRow> rows = new ArrayList<>();
            for (int count : updateCounts) {
                total += count;
                ValueRow valueRow = new ValueRow(1);
                valueRow.setColumn(1, new SQLLongint(count));
                rows.add(valueRow);
            }
            assert total == operationContext.getRecordsWritten();
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(rows, 1));
        }
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txnView = childTxn;
    }

    @Override
    public TxnView getTxn(){
        if(txnView==null)
            return operationContext.getTxn();
        else
            return txnView;
    }

    @Override
    public byte[] getDestinationTable(){
        return ((DMLWriteOperation)operationContext.getOperation()).getDestinationTable();
    }
}
