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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.delete.DeletePipelineWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import scala.util.Either;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class DeleteDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K,Either<Exception, V>> rdd;
    private final OperationContext operationContext;
    private final Configuration conf;
    private TxnView txnView;

    public DeleteDataSetWriter(JavaPairRDD<K, Either<Exception, V>> rdd, OperationContext operationContext, Configuration conf){
        this.rdd=rdd;
        this.operationContext=operationContext;
        this.conf=conf;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf);
        if (operationContext.getOperation() != null) {
            operationContext.getOperation().fireAfterStatementTriggers();
        }
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(new LocatedRow(valueRow)), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txnView = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        TxnView txn=txnView;
        long conglom = Long.parseLong(Bytes.toString(((DMLWriteOperation)operationContext.getOperation()).getDestinationTable()));
        return new DeletePipelineWriter(txn,conglom,operationContext);
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
