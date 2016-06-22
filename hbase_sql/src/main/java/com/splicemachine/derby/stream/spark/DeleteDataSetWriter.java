package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
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
        return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
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
