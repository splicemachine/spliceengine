package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkUpdateDataSetWriter<K,V> implements DataSetWriter{
    private JavaPairRDD<K,V> rdd;
    private final OperationContext operationContext;
    private final Configuration conf;

    public SparkUpdateDataSetWriter(JavaPairRDD<K, V> rdd,OperationContext operationContext,Configuration conf){
        this.rdd=rdd;
        this.operationContext=operationContext;
        this.conf=conf;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf); //actually does the writing
        operationContext.getOperation().fireAfterStatementTriggers();
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLInteger((int)operationContext.getRecordsWritten()));
        return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
    }

    @Override
    public void setTxn(TxnView childTxn){
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
