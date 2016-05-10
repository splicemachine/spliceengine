package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectPipelineWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.util.Either;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDirectDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K, Either<Exception, V>> rdd;
    private final JavaSparkContext context;
    private final OperationContext opContext;
    private final Configuration conf;
    private boolean skipIndex;
    private long destConglom;
    private TxnView txn;

    public SparkDirectDataSetWriter(JavaPairRDD<K, Either<Exception, V>> rdd,
                                    JavaSparkContext context,
                                    OperationContext opContext,
                                    Configuration conf,
                                    boolean skipIndex,
                                    long destConglom,
                                    TxnView txn){
        this.rdd=rdd;
        this.context=context;
        this.opContext=opContext;
        this.conf=conf;
        this.skipIndex=skipIndex;
        this.destConglom=destConglom;
        this.txn=txn;
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
        this.txn = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new DirectPipelineWriter(destConglom,txn,opContext,skipIndex);
    }

    @Override
    public TxnView getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(destConglom);
    }
}
