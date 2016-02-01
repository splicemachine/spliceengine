package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.IndexTableScannerBuilder;
import com.splicemachine.derby.stream.function.HTableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.stream.index.HTableInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkIndexScanBuilder<V> extends IndexTableScannerBuilder<V>{
    private String tableName;

    public SparkIndexScanBuilder(){
    }

    public SparkIndexScanBuilder(String tableName){
        this.tableName=tableName;
    }

    @Override
    public DataSet<V> buildDataSet() throws StandardException{
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO,getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<byte[], KVPair> rawRDD = ctx.newAPIHadoopRDD(conf, HTableInputFormat.class,
                byte[].class, KVPair.class);

        Function f=new SparkSpliceFunctionWrapper<>(new HTableScanTupleFunction<>());
        return new SparkDataSet<>(rawRDD.map(f));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        tableName = in.readUTF();
    }
}
