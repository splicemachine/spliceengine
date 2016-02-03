package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;


/**
 * Spark-based DataSetProcessor.
 *
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

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException{
        return new SparkScanSetBuilder<>(this,tableName);
    }

    @Override
    public <Op extends SpliceOperation,V> IndexScanSetBuilder<V> newIndexScanSet(Op spliceOperation,String tableName) throws StandardException{
        return new SparkIndexScanBuilder<>(tableName);
    }

    @Override
    public <V> DataSet<V> getEmpty() {
        return getEmpty(SparkConstants.RDD_NAME_EMPTY_DATA_SET);
    }

    @Override
    public <V> DataSet<V> getEmpty(String name) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1), name);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.singletonList(value), 1);
        rdd1.setName(SparkConstants.RDD_NAME_SINGLE_ROW_DATA_SET);
        return new SparkDataSet<>(rdd1);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, SpliceOperation op, boolean isLast) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.singletonList(value), 1);
        rdd1.setName(isLast ? op.getPrettyExplainPlan() : SparkConstants.RDD_NAME_SINGLE_ROW_DATA_SET);
        return new SparkDataSet<>(rdd1);
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation) {
        setupBroadcastedActivation(spliceOperation.getActivation());
        OperationContext<Op> operationContext =new SparkOperationContext<>(spliceOperation,broadcastedActivation.get());
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive();
            operationContext.setFailBadRecordCount(failBadRecordCount);
        }
        return operationContext;
    }


    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        setupBroadcastedActivation(activation);
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
            FileInfo contentSummary = ImportUtils.getImportDataSize(path);
            SpliceSpark.pushScope((op != null ? op.getSparkStageName() + ": " : "") +
                SparkConstants.SCOPE_NAME_READ_TEXT_FILE + "\n" +
                "{file=" + String.format(path) + ", " +
                "size=" + contentSummary.spaceConsumed() + ", " +
                "files=" + contentSummary.fileCount());
            return new SparkPairDataSet<>(SpliceSpark.getContext().newAPIHadoopFile(
                path, WholeTextInputFormat.class, String.class, InputStream.class,HConfiguration.INSTANCE.unwrapDelegate()));
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
            FileInfo contentSummary = ImportUtils.getImportDataSize(path);
            SpliceSpark.pushScope((op != null ? op.getSparkStageName() + ": " : "") +
                SparkConstants.SCOPE_NAME_READ_TEXT_FILE + "\n" +
                "{file=" +path+ ", " +
                "size=" + contentSummary.spaceConsumed() + ", " +
                "files=" + contentSummary.fileCount() + "}");
            JavaRDD rdd = SpliceSpark.getContext().textFile(path);
            return new SparkDataSet<>(rdd,SparkConstants.RDD_NAME_READ_TEXT_FILE);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(Collections.<Tuple2<K,V>>emptyList(), 1));
    }

    @Override
    public <V> DataSet< V> createDataSet(Iterable<V> value) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }

    @Override
    public <K, V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(Arrays.<Tuple2<K, V>>asList(new Tuple2(key, value)), 1));
    }

    @Override
    public void setPermissive() {
        this.permissive = true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount) {
        this.failBadRecordCount = failBadRecordCount;
    }


    @Override
    public void clearBroadcastedOperation(){
        broadcastedActivation.remove();
    }


    private ThreadLocal<BroadcastedActivation> broadcastedActivation = new ThreadLocal<>();

    private void setupBroadcastedActivation(Activation activation){
        if(broadcastedActivation.get()==null){
            broadcastedActivation.set(new BroadcastedActivation(activation));
        }
    }
}
