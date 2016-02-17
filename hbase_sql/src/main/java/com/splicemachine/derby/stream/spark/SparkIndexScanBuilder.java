package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.actions.IndexConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.ScopeNamed;
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
        return buildDataSet(null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> buildDataSet(Object caller) throws StandardException{
        // This code correlated somewhat to SparkDataSetProcessor.getHTableScanner from master_dataset.
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO,getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }

        String scope = null;
        if (caller instanceof String)
            scope = (String)caller;
        else if (caller instanceof IndexConstantOperation)
            scope = ((IndexConstantOperation)caller).getScopeName();
        else if (caller instanceof ScopeNamed)
            scope = ((ScopeNamed)caller).getScopeName();
        else if (caller instanceof ConstantAction)
            scope = "Scan Table";

        SpliceSpark.pushScope(scope + ": Scan");
        JavaPairRDD<byte[], KVPair> rawRDD = ctx.newAPIHadoopRDD(
            conf, HTableInputFormat.class, byte[].class, KVPair.class);
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();

        SpliceSpark.pushScope(scope + ": Deserialize");
        HTableScanTupleFunction f1 = new HTableScanTupleFunction();
        Function f2 = new SparkSpliceFunctionWrapper<>(f1);
        try {
            return new SparkDataSet(rawRDD.map(f2), f1.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
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
