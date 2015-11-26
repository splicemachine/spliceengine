package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.load.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.HTableScanTupleFunction;
import com.splicemachine.derby.stream.index.HTableInputFormat;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;
import com.splicemachine.db.iapi.types.RowLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.splicemachine.db.iapi.sql.Activation;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;


/**
 * Created by jleach on 4/13/15.
 */
public class SparkDataSetProcessor implements DataSetProcessor, Serializable {
    private int failBadRecordCount = -1;
    private boolean permissive;

    public SparkDataSetProcessor() {

    }

    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(final Op spliceOperation, TableScannerBuilder siTableBuilder, TableName tableName) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName.getNameAsString());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        
        String displayableTableName = getDisplayableTableName(spliceOperation, tableName);
        SpliceSpark.pushScope(spliceOperation.getSparkStageName() + ": Table " + displayableTableName);
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMInputFormat.class, RowLocation.class, ExecRow.class);
        rawRDD.setName(String.format(SparkConstants.RDD_NAME_SCAN_TABLE, displayableTableName));
        SpliceSpark.popScope();
        
        SpliceSpark.pushScope(spliceOperation.getSparkStageName() + ": Deserialize");
        TableScanTupleFunction<Op> f = new TableScanTupleFunction<Op>(createOperationContext(spliceOperation));
        JavaRDD<LocatedRow> appliedRDD = rawRDD.map(f);
        appliedRDD.setName(spliceOperation.getPrettyExplainPlan());
        SpliceSpark.popScope();
        
        return new SparkDataSet(appliedRDD);
    }

    private String getDisplayableTableName(SpliceOperation spliceOperation, String conglomId) {
        String displayableTableName;
        if (spliceOperation instanceof ScanOperation) {
            displayableTableName = ((ScanOperation)spliceOperation).getDisplayableTableName();
            if (displayableTableName == null || displayableTableName.isEmpty())
                displayableTableName = conglomId;
        } else {
            displayableTableName = conglomId;
        }
        return displayableTableName;
    }
    
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(final Activation activation, TableScannerBuilder siTableBuilder, TableName tableName) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName.getNameAsString());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(conf, SMInputFormat.class,
                RowLocation.class, ExecRow.class);

        return new SparkDataSet(rawRDD.map(
                new TableScanTupleFunction(createOperationContext(activation))));
    }

    @Override
    public <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, TableName tableName) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName.getNameAsString());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, hTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<byte[], KVPair> rawRDD = ctx.newAPIHadoopRDD(conf, HTableInputFormat.class,
                byte[].class, KVPair.class);

        return new SparkDataSet(rawRDD.map(
                new HTableScanTupleFunction()));
    }
    
    @Override
    public <V> DataSet<V> getEmpty() {
        return getEmpty(SparkConstants.RDD_NAME_EMPTY_DATA_SET);
    }

    @Override
    public <V> DataSet<V> getEmpty(String name) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1), name);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.<V>singletonList(value), 1);
        rdd1.setName(SparkConstants.RDD_NAME_SINGLE_ROW_DATA_SET);
        return new SparkDataSet(rdd1);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, SpliceOperation op, boolean isLast) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.<V>singletonList(value), 1);
        rdd1.setName(isLast ? op.getPrettyExplainPlan() : SparkConstants.RDD_NAME_SINGLE_ROW_DATA_SET);
        return new SparkDataSet(rdd1);
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation) {
        OperationContext<Op> operationContext = new SparkOperationContext<Op>(spliceOperation);
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive();
            operationContext.setFailBadRecordCount(failBadRecordCount);
        }
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        return new SparkOperationContext<Op>(activation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
        SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
    }

    public void setSchedulerPool(String pool) {
        SpliceSpark.getContext().setLocalProperty("spark.scheduler.pool",pool);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path) {
        return readWholeTextFile(path);
    }
    
    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path, SpliceOperation op) {
        try {
            ContentSummary contentSummary = ImportUtils.getImportDataSize(new Path(path));
            SpliceSpark.pushScope((op != null ? op.getSparkStageName() + ": " : "") +
                SparkConstants.SCOPE_NAME_READ_TEXT_FILE + "\n" +
                "{file=" + String.format(path) + ", " +
                "size=" + contentSummary.getSpaceConsumed() + ", " +
                "files=" + contentSummary.getFileCount());
            return new SparkPairDataSet<>(SpliceSpark.getContext().newAPIHadoopFile(
                path, WholeTextInputFormat.class, String.class, InputStream.class, SpliceConstants.config));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public DataSet<String> readTextFile(String path) {
        return readTextFile(path, null);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<String> readTextFile(String path, SpliceOperation op) {
        try {
            ContentSummary contentSummary = ImportUtils.getImportDataSize(new Path(path));
            SpliceSpark.pushScope((op != null ? op.getSparkStageName() + ": " : "") +
                SparkConstants.SCOPE_NAME_READ_TEXT_FILE + "\n" +
                "{file=" + String.format(path) + ", " +
                "size=" + contentSummary.getSpaceConsumed() + ", " +
                "files=" + contentSummary.getFileCount() + "}");
            JavaRDD rdd = SpliceSpark.getContext().textFile(path);
            return new SparkDataSet<String>(rdd, SparkConstants.RDD_NAME_READ_TEXT_FILE);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(Collections.<Tuple2<K,V>>emptyList(), 1));
    }

    @Override
    public <V> DataSet< V> createDataSet(Iterable<V> value) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }

    @Override
    public <K, V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(Arrays.<Tuple2<K, V>>asList(new Tuple2(key, value)), 1));
    }

    @Override
    public void setPermissive() {
        this.permissive = true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount) {
        this.failBadRecordCount = failBadRecordCount;
    }
}
