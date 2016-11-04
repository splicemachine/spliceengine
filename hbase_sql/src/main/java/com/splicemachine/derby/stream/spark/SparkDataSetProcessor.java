/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.api.core.SMTextInputFormat;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Spark-based DataSetProcessor.
 *
 */
public class SparkDataSetProcessor implements DistributedDataSetProcessor, Serializable {
    private long failBadRecordCount = -1;
    private boolean permissive;
    private String statusDirectory;
    private String importFileName;

    private static final Logger LOG = Logger.getLogger(SparkDataSetProcessor.class);

    public SparkDataSetProcessor() {
    }

    @Override
    public Type getType() {
        return Type.SPARK;
    }

    @Override
    public void setup(Activation activation,String description,String schedulerPool) throws StandardException{
        String sql = activation.getPreparedStatement().getSource();
        long txnId = getCurrentTransaction(activation).getTxnId();
        sql = (sql == null) ? description : sql;
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobName = userId + " <" + txnId + ">";
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setup(): jobName = %s", jobName);
        setJobGroup(jobName,sql);
        setSchedulerPool(schedulerPool);
    }

    private static TxnView getCurrentTransaction(Activation activation) throws StandardException {
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
    }

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException{
        return new SparkScanSetBuilder<>(this,tableName,spliceOperation); // tableName = conglomerate number
    }

    @Override
    public <V> DataSet<V> getEmpty() {
        return getEmpty(RDDName.EMPTY_DATA_SET.displayName());
    }

    @Override
    public <V> DataSet<V> getEmpty(String name) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1), name);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value) {
        return singleRowDataSet(value, "Finalize Result");
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        String scope = StreamUtils.getScopeString(caller);
        SpliceSpark.pushScope(scope);
        try {
            JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.singletonList(value), 1);
            rdd1.setName(RDDName.SINGLE_ROW_DATA_SET.displayName());
            return new SparkDataSet<>(rdd1);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation) {
        setupBroadcastedActivation(spliceOperation.getActivation(), spliceOperation);
        OperationContext<Op> operationContext =new SparkOperationContext<>(spliceOperation,broadcastedActivation.get());
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive(statusDirectory, importFileName, failBadRecordCount);
        }
        return operationContext;
    }


    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        if (activation !=null) {
            return new SparkOperationContext<>(activation, broadcastedActivation.get());
        } else {
            return new SparkOperationContext<>(activation, null);
        }
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("setJobGroup(): jobName=%s, jobDescription=%s", jobName, jobDescription));
        SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
    }

    public void setSchedulerPool(String pool) {
        SpliceSpark.getContext().setLocalProperty("spark.scheduler.pool",pool);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path) throws StandardException {
        return readWholeTextFile(path,null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path, SpliceOperation op) throws StandardException {
        try {
            FileInfo fileInfo = ImportUtils.getImportFileInfo(path);
            String displayString="";
            if(op!=null)
                displayString = op.getScopeName()+": "+OperationContext.Scope.READ_TEXT_FILE.displayName();
            SpliceSpark.pushScope(displayString);
            JavaPairRDD rdd = SpliceSpark.getContext().newAPIHadoopFile(
                    path,
                    WholeTextInputFormat.class,
                    String.class,
                    InputStream.class,
                    HConfiguration.unwrapDelegate());
            return new SparkPairDataSet<>(rdd,OperationContext.Scope.READ_TEXT_FILE.displayName());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public DataSet<String> readTextFile(String path) throws StandardException {
        return readTextFile(path, null);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<String> readTextFile(String path, SpliceOperation op) throws StandardException {
        try {
            FileInfo fileInfo = ImportUtils.getImportFileInfo(path);
            String displayString="";
            if(op!=null)
                displayString = op.getScopeName()+": "+OperationContext.Scope.READ_TEXT_FILE.displayName();

            SpliceSpark.pushScope(displayString);
            JavaPairRDD<LongWritable, Text> pairRdd=SpliceSpark.getContext().newAPIHadoopFile(
                    path,
                    SMTextInputFormat.class,
                    LongWritable.class,
                    Text.class,
                    new Configuration(HConfiguration.unwrapDelegate()));

            JavaRDD rdd =pairRdd.values()
                    .map(new Function<Text,String>() {
                        @Override
                        public String call(Text o) throws Exception {
                            return o.toString();
                        }
                    });
            SparkUtils.setAncestorRDDNames(rdd, 1, new String[] {fileInfo.toSummary()}, null);
            return new SparkDataSet<>(rdd,OperationContext.Scope.READ_TEXT_FILE.displayName());
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
    public <V> DataSet< V> createDataSet(Iterator<V> value) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet< V> createDataSet(Iterator<V> value, String name) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Lists.newArrayList(value));
        rdd1.setName(name);
        return new SparkDataSet(rdd1);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <K, V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(Arrays.<Tuple2<K, V>>asList(new Tuple2(key, value)), 1));
    }

    @Override
    public void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold) {
        this.permissive = true;
        this.statusDirectory = statusDirectory;
        this.importFileName = importFileName;
        this.failBadRecordCount = badRecordThreshold;
    }


    @Override
    public void clearBroadcastedOperation(){
        broadcastedActivation.remove();
    }

    @Override
    public void stopJobGroup(String jobName) {
        SpliceSpark.getContext().cancelJobGroup(jobName);
    }

    private transient ThreadLocal<BroadcastedActivation> broadcastedActivation = new ThreadLocal<>();

    private void setupBroadcastedActivation(Activation activation, SpliceOperation root){
        if(broadcastedActivation.get()==null){
            broadcastedActivation.set(new BroadcastedActivation(activation, root));
        }
    }

    @Override
    public Partitioner getPartitioner(DataSet<LocatedRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return new HBasePartitioner(dataSet, template, keyDecodingMap, keyOrder, rightHashKeys);
    }


}
