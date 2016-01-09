package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.load.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.HTableScanTupleFunction;
import com.splicemachine.derby.stream.function.TxnViewDecoderFunction;
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
import com.splicemachine.mrio.api.core.SMTxnInputFormat;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.coprocessor.TxnMessage;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
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
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(
        final Op spliceOperation, TableScannerBuilder siTableBuilder, String conglomerateId) throws StandardException {
        
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomerateId);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, com.splicemachine.mrio.MRConstants.DEFAULT_SPLICE_JDBC_STR_VALUE);
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        
        String scope = getScopeName(spliceOperation, siTableBuilder, conglomerateId); 
        SpliceSpark.pushScope(scope);
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMInputFormat.class, RowLocation.class, ExecRow.class);
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();
        
        SpliceSpark.pushScope(scope + ": Deserialize");
        TableScanTupleFunction<Op> f = new TableScanTupleFunction<Op>(createOperationContext(spliceOperation));
        try {
            return new SparkDataSet(rawRDD.map(f), spliceOperation.getPrettyExplainPlan());
        } finally {
            SpliceSpark.popScope();
        }
    }

    private String getScopeName(
        final SpliceOperation spliceOperation, TableScannerBuilder siTableBuilder, String conglomerateId) {

        StringBuffer sb = new StringBuffer();
        if (spliceOperation instanceof ScanOperation) {
            ScanOperation scanOp = ((ScanOperation)spliceOperation);
            if (scanOp.isIndexScan()) {
                sb.append("Scan Index ").append(scanOp.getIndexDisplayName());
                sb.append(" (Table ").append(scanOp.getTableDisplayName()).append(")");
            } else {
                sb.append("Scan Table ").append(scanOp.getTableDisplayName());
            }
        } else {
            sb.append(spliceOperation.getSparkStageName());
            sb.append(conglomerateId);
        }
        
        return sb.toString();
    }
    
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(
        final Activation activation, TableScannerBuilder siTableBuilder, String conglomerateId) throws StandardException {
        return getTableScanner(activation, siTableBuilder, conglomerateId, null, null);
    }
    
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(
        final Activation activation, TableScannerBuilder siTableBuilder, String conglomerateId, String tableDisplayName, String callerName) throws StandardException {
        
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomerateId);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, com.splicemachine.mrio.MRConstants.DEFAULT_SPLICE_JDBC_STR_VALUE);
        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }

        String finalDisplayName = tableDisplayName != null ? tableDisplayName : conglomerateId;
        SpliceSpark.pushScope((callerName != null ? callerName + ": " : "") + "Scan table " + finalDisplayName);
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMInputFormat.class, RowLocation.class, ExecRow.class);
        rawRDD.setName(String.format(SparkConstants.RDD_NAME_SCAN_TABLE, finalDisplayName));
        SpliceSpark.popScope();

        SpliceSpark.pushScope((callerName != null ? callerName + ": " : "") + "Deserialize");
        TableScanTupleFunction f = new TableScanTupleFunction(createOperationContext(activation));
        try {
            return new SparkDataSet(rawRDD.map(f), f.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, String conglomerateId) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomerateId);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, com.splicemachine.mrio.MRConstants.DEFAULT_SPLICE_JDBC_STR_VALUE);
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, hTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }

        SpliceSpark.pushScope("Scan table");
        JavaPairRDD<byte[], KVPair> rawRDD = ctx.newAPIHadoopRDD(conf, HTableInputFormat.class,
            byte[].class, KVPair.class);
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();

        HTableScanTupleFunction f = new HTableScanTupleFunction();
        try {
            return new SparkDataSet(rawRDD.map(f), f.getPrettyFunctionName());
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public DataSet<TxnView> getTxnTableScanner(long beforeTS, long afterTS, byte[] destinationTable) {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(MRConstants.SPLICE_INPUT_TABLE_NAME, HBaseTableInfoFactory.getInstance().getTableInfo(SIConstants.TRANSACTION_TABLE).getNameAsString());
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, com.splicemachine.mrio.MRConstants.DEFAULT_SPLICE_JDBC_STR_VALUE);
        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
        conf.setLong(MRConstants.SPLICE_TXN_MIN_TIMESTAMP, afterTS);
        conf.setLong(MRConstants.SPLICE_TXN_MAX_TIMESTAMP, beforeTS);
        conf.set(MRConstants.SPLICE_TXN_DEST_TABLE, Bytes.toString(destinationTable));
        
        SpliceSpark.pushScope("Scan table");
        JavaPairRDD<RowLocation, TxnMessage.Txn> rawRDD = ctx.newAPIHadoopRDD(
            conf, SMTxnInputFormat.class, RowLocation.class, TxnMessage.Txn.class);
        rawRDD.setName("Perform Scan");
        SpliceSpark.popScope();

        SpliceSpark.pushScope("Deserialize");
        HTableScanTupleFunction f1 = new HTableScanTupleFunction();
        JavaRDD rdd2 = rawRDD.map(f1);
        
        try {
            return new ControlDataSet<TxnMessage.Txn>(rdd2.collect()).map(new TxnViewDecoderFunction());
        } finally {
            SpliceSpark.popScope();
        }
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
        return singleRowDataSet(value, "Finalize Result");
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        String scope = null;
        if (caller instanceof String)
            scope = (String)caller;
        else if (caller instanceof SpliceOperation) 
            scope = ((SpliceOperation)caller).getSparkStageName();
        else
            scope = "Finalize Result";
        
        SpliceSpark.pushScope(scope);
        try {
            JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.<V>singletonList(value), 1);
            rdd1.setName(SparkConstants.RDD_NAME_SINGLE_ROW_DATA_SET);
            return new SparkDataSet(rdd1);
        } finally {
            SpliceSpark.popScope();
        }
    }`

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
                "size=" + FileUtils.byteCountToDisplaySize(contentSummary.getSpaceConsumed()) + ", " +
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
                "size=" + FileUtils.byteCountToDisplaySize(contentSummary.getSpaceConsumed()) + ", " +
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
