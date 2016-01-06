package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.load.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.iapi.*;


import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;


/**
 * Created by jleach on 4/13/15.
 */
public class SparkDataSetProcessor implements DistributedDataSetProcessor, Serializable {
    private int failBadRecordCount = -1;
    private boolean permissive;

    public SparkDataSetProcessor() {

    }

    @Override
    public void setup(Activation activation,String description,String schedulerPool) throws StandardException{
        // TODO (wjk): this is mostly a copy/paste from SpliceBaseOperation.openCore() - consolidate?
        String sql = activation.getPreparedStatement().getSource();
        long txnId = getCurrentTransaction(activation).getTxnId();
        sql = (sql == null) ? description : sql;
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobName = userId + " <" + txnId + ">";
        setJobGroup(jobName,sql);
        setSchedulerPool(schedulerPool);
    }

    private static TxnView getCurrentTransaction(Activation activation) throws StandardException {
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
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

//    @Override
//    public <V> DataSet<V> getTableScanner(final Activation activation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
//        JavaSparkContext ctx = SpliceSpark.getContext();
//        Configuration conf = new Configuration(SIConstants.config);
//        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
//        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
//        conf.set(MRConstants.ONE_SPLIT_PER_REGION, "true");
//        try {
//            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
//        } catch (IOException ioe) {
//            throw StandardException.unexpectedUserException(ioe);
//        }
//        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(conf, SMInputFormat.class,
//                RowLocation.class, ExecRow.class);
//
//        return new SparkDataSet(rawRDD.map(
//                new TableScanTupleFunction(createOperationContext(activation))));
//    }

//    @Override
//    public <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, String tableName) throws StandardException {
//        JavaSparkContext ctx = SpliceSpark.getContext();
//        Configuration conf = new Configuration(SIConstants.config);
//        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
//        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
//        try {
//            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, hTableBuilder.getTableScannerBuilderBase64String());
//        } catch (IOException ioe) {
//            throw StandardException.unexpectedUserException(ioe);
//        }
//        JavaPairRDD<byte[], KVPair> rawRDD = ctx.newAPIHadoopRDD(conf, HTableInputFormat.class,
//                byte[].class, KVPair.class);
//
//        return new SparkDataSet(rawRDD.map(new HTableScanTupleFunction()));
//    }
    
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
        OperationContext<Op> operationContext =new SparkOperationContext<>(spliceOperation);
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive();
            operationContext.setFailBadRecordCount(failBadRecordCount);
        }
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        return new SparkOperationContext<>(activation);
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
        return readWholeTextFile(path,null);
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
