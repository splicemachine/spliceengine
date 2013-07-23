package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.Task;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Cleaner Utility for keeping SYS_TEMP from growing without bound under each operation.
 *
 * This class asynchronously submits a Task that cleans up the SYS_TEMP regions that are requested, without
 * blocking any further action from being taken.
 *
 * @author Scott Fines
 * Created on: 4/10/13
 */
public class TempCleaner {
    private static final Logger LOG = Logger.getLogger(TempCleaner.class);
    private static final int DEFAULT_CLEAN_TASK_PRIORITY = 2;
    private static final String CLEANER_JOBS = "splice.temp.maxCleanerThreads";
    private static final int DEFAULT_CLEANER_JOBS = 4;
    private final ExecutorService cleanWatcher;
    private final int taskPriority;

    public TempCleaner(Configuration configuration) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("tempCleaner")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        SpliceLogUtils.error(LOG, "Unexpected error cleaning temp table", e);
                    }
                }).build();
        int cleanerThreads = configuration.getInt(CLEANER_JOBS, DEFAULT_CLEANER_JOBS);
        cleanWatcher = new ThreadPoolExecutor(1,cleanerThreads,60,
                TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(),factory);
        this.taskPriority = configuration.getInt("splice.temp.cleanTaskPriority",DEFAULT_CLEAN_TASK_PRIORITY);
    }

    /**
     * Submit a range of rows in SYS_TEMP to be deleted.
     *
     * @param start the start row to be deleted
     * @param finish the finish row to be deleted
     */
    public void deleteRange(byte[] uid, byte[] start, byte[] finish) throws StandardException {
        if(LOG.isTraceEnabled())
            LOG.trace("cleaning temp space for task "+ uid);
        final TempCleanJob job = new TempCleanJob(uid,start,finish,taskPriority);
        cleanWatcher.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                JobFuture future = SpliceDriver.driver().getJobScheduler().submit(job);
                try{
                    future.completeAll();
                }finally{
                    future.cleanup();
                }

                /*
                 * If a region is entirely contained within the start and finish keys, then
                 * it can be removed. Unfortunately, HBASE-7403 (https://issues.apache.org/jira/browse/HBASE-7403)
                 * indicates that this is not a feature which is available until 0.95 at least (which is
                 * not available for Cloudera 4.2.1). When that feature becomes standard, then we
                 * can merge regions here as well.
                 */
                return null;
            }
        });
    }

    public static class TempCleanJob implements CoprocessorJob {
        private TempCleanTask task;
        private final String jobId;

        public TempCleanJob(byte[] uid,byte[] start, byte[] finish,int taskPriority){
            this.jobId = new String(uid);
            Scan scan = new Scan();
            scan.setStartRow(start);
            scan.setStopRow(finish);
            this.task = new TempCleanTask(jobId,scan,taskPriority);
        }

        @Override
        public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
            return Collections.singletonMap(task,Pair.newPair(task.scan.getStartRow(),task.scan.getStopRow()));
        }

        @Override
        public HTableInterface getTable() {
            return SpliceAccessManager.getHTable(SpliceOperationCoprocessor.TEMP_TABLE);
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
            return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
        }

        @Override
        public TransactionId getParentTransaction() {
            return null;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

    public static class TempCleanTask extends ZkTask {
        private Scan scan;
        private RegionScanner scanner;
        private HRegion region;

        public TempCleanTask(){}

        public TempCleanTask(String jobId,Scan scan,int priority){
            super(jobId,priority,null,false); // we are non-transactional
            this.scan = scan;
        }

        @Override
        protected String getTaskType() {
            return "tempCleanTask";
        }

        @Override
        public boolean invalidateOnClose() {
            return true;
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            List<KeyValue> keys = Lists.newArrayListWithCapacity(1);

            try {
                while(scanner.next(keys)){
                    Delete delete = new Delete(keys.get(0).getRow());
                    region.delete(delete,true);
                    keys.clear();
                }
                if(keys.size()>0){
                    Delete delete = new Delete(keys.get(0).getRow());
                    region.delete(delete,true);
                }
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
        }

        @Override
        public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
            this.region = rce.getRegion();
            try {
                this.scanner = region.getScanner(scan);
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
            super.prepareTask(rce, zooKeeper);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            scan.write(out);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            scan = new Scan();
            scan.readFields(in);
        }
    }
}
