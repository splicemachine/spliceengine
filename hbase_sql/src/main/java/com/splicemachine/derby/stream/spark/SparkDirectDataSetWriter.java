package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDirectDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K, V> rdd;
    private final JavaSparkContext context;
    private final Configuration conf;

    public SparkDirectDataSetWriter(JavaPairRDD<K, V> rdd,JavaSparkContext context,Configuration conf){

        this.rdd=rdd;
        this.context=context;
        this.conf=conf;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf);
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLInteger(0));
        return new SparkDataSet<>(context.parallelize(Collections.singletonList(new LocatedRow(valueRow))));
    }

    @Override
    public void setTxn(TxnView childTxn){
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
