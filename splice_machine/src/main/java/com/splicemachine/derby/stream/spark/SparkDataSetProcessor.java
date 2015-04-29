package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.mrio.api.core.SMInputFormat;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;

/**
 * Created by jleach on 4/13/15.
 */
public class SparkDataSetProcessor <Op extends SpliceOperation,K,V> implements DataSetProcessor<Op,K,V>, Serializable {

    public SparkDataSetProcessor() {

    }

    @Override
    public DataSet< V> getTableScanner(final Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_CONGLOMERATE, tableName);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;create=true;user=splice;password=admin");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(conf, SMInputFormat.class,
                RowLocation.class, ExecRow.class);

        return new SparkDataSet(rawRDD.map(
                new TableScanTupleFunction<Op>(createOperationContext(spliceOperation))));
    }

    @Override
    public DataSet<V> getEmpty() {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1));
    }

    @Override
    public DataSet<V>  singleRowDataSet(V value) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Collections.<V>singletonList(value),1));
    }

    @Override
    public OperationContext<Op> createOperationContext(Op spliceOperation) {
        return new SparkOperationContext<Op>(spliceOperation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
        SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
    }

    @Override
    public DataSet< V> createDataSet(Iterable<V> value) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }
}