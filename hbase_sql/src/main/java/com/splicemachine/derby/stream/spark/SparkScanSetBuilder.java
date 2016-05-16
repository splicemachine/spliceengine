package com.splicemachine.derby.stream.spark;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkScanSetBuilder<V> extends TableScannerBuilder<V> {
    private String tableName;
    private SparkDataSetProcessor dsp;
    private SpliceOperation op;

    public SparkScanSetBuilder(){
    }

    public SparkScanSetBuilder(SparkDataSetProcessor dsp,String tableName,SpliceOperation op){
        this.tableName=tableName;
        this.dsp = dsp;
        this.op = op;
    }

    @Override
    public DataSet<V> buildDataSet() throws StandardException {
        return buildDataSet("Scan Table");
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSet<V> buildDataSet(Object caller) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(HConfiguration.unwrapDelegate());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        if (oneSplitPerRegion) {
            conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        }
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO,getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }

        String scopePrefix = StreamUtils.getScopeString(caller);
        SpliceSpark.pushScope(String.format("%s: Scan", scopePrefix));
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMInputFormat.class, RowLocation.class, ExecRow.class);
        // rawRDD.setName(String.format(SparkConstants.RDD_NAME_SCAN_TABLE, tableDisplayName));
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();

        OperationContext<SpliceOperation> operationContext;
        if (op != null)
            operationContext = dsp.createOperationContext(op);
        else
            operationContext = dsp.createOperationContext(activation);

        SparkSpliceFunctionWrapper f = new SparkSpliceFunctionWrapper<>(new TableScanTupleFunction<>(operationContext));
        SpliceSpark.pushScope(String.format("%s: Deserialize", scopePrefix));
        try {
            return new SparkDataSet<>(rawRDD.map(f), op != null ? op.getPrettyExplainPlan() : f.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeObject(dsp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.tableName = in.readUTF();
        this.dsp = (SparkDataSetProcessor)in.readObject();
    }
}
