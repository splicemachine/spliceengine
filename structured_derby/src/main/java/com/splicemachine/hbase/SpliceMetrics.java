package com.splicemachine.hbase;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.job.TaskSchedulerManagement;
import com.splicemachine.tools.ConnectionPool;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * @author Scott Fines
 * Created on: 4/10/13
 */
public class SpliceMetrics implements Updater {
    private final MetricsRecord taskMetrics;
    private final MetricsRecord jobMetrics;
    private final MetricsRecord writerMetrics;
    private final MetricsRecord poolMetrics;

    private MetricsRegistry taskRegistry = new MetricsRegistry();
    private MetricsRegistry jobRegistry = new MetricsRegistry();
    private MetricsRegistry writerRegistry = new MetricsRegistry();
    private MetricsRegistry poolRegistry = new MetricsRegistry();

    /*Task scheduler metrics*/
    private final MetricsIntValue numPendingTasks = new MetricsIntValue("numPending",taskRegistry);
    private final MetricsIntValue currentTaskWorkers = new MetricsIntValue("currentWorkers",taskRegistry);
    private final MetricsIntValue maxTaskWorkers = new MetricsIntValue("maxWorkers",taskRegistry);
    private final MetricsIntValue numRunningTasks = new MetricsIntValue("numRunning",taskRegistry);
    private final MetricsLongValue totalSubmittedTasks = new MetricsLongValue("totalSubmitted",taskRegistry);
    private final MetricsLongValue totalCompletedTasks = new MetricsLongValue("totalCompleted",taskRegistry);
    private final MetricsLongValue totalFailedTasks = new MetricsLongValue("totalFailed",taskRegistry);
    private final MetricsLongValue totalInvalidatedTasks = new MetricsLongValue("totalInvalidated",taskRegistry);
    private final MetricsLongValue totalCancelledTasks = new MetricsLongValue("totalCancelled",taskRegistry);

    /*Job scheduler metrics*/
    private final MetricsLongValue totalSubmittedJobs = new MetricsLongValue("totalSubmitted",jobRegistry);
    private final MetricsLongValue totalCompletedJobs = new MetricsLongValue("totalCompleted",jobRegistry);
    private final MetricsLongValue totalFailedJobs = new MetricsLongValue("totalFailed",jobRegistry);
    private final MetricsLongValue totalCancelledJobs = new MetricsLongValue("totalCancelled",jobRegistry);
    private final MetricsIntValue numRunningJobs = new MetricsIntValue("numRunning",jobRegistry);

    /*Table Writer metrics*/
    private final MetricsLongValue maxBufferHeapSizeWriter = new MetricsLongValue("maxBufferHeapSize",writerRegistry);
    private final MetricsIntValue maxBufferEntriesWriter = new MetricsIntValue("maxBufferEntries",writerRegistry);
    private final MetricsIntValue maxFlushesPerBufferWriter = new MetricsIntValue("maxFlushesPerBuffer",writerRegistry);
    private final MetricsIntValue outstandingCallBuffersWriter = new MetricsIntValue("outstandingCallBuffers",writerRegistry);
    private final MetricsIntValue pendingBufferFlushesWriter = new MetricsIntValue("pendingBufferFlushes",writerRegistry);
    private final MetricsIntValue executingBufferFlushesWriter = new MetricsIntValue("executingBufferFlushes",writerRegistry);
    private final MetricsIntValue runningWriteThreadsWriter = new MetricsIntValue("runningWriteThreads",writerRegistry);
    private final MetricsLongValue totalBufferFlushesWriter = new MetricsLongValue("totalBufferFlushes",writerRegistry);
    private final MetricsLongValue cachedTablesWriter = new MetricsLongValue("cachedTables",writerRegistry);
    private final MetricsLongValue cacheLastUpdatedWriter = new MetricsLongValue("cacheLastUpdated",writerRegistry);
    private final MetricsIntValue compressedWritesWriter = new MetricsIntValue("compressedWrites",writerRegistry);

    /*Connection Pool Metrics*/
    private final MetricsIntValue poolWaiting = new MetricsIntValue("waiting",poolRegistry);
    private final MetricsIntValue maxPoolSize = new MetricsIntValue("maxPoolSize",poolRegistry);
    private final MetricsIntValue poolAvailable = new MetricsIntValue("available",poolRegistry);
    private final MetricsIntValue poolInUse = new MetricsIntValue("inUse",poolRegistry);

    public SpliceMetrics() {
        MetricsContext context = MetricsUtil.getContext("splice");
        taskMetrics = MetricsUtil.createRecord(context,"tasks");
        jobMetrics = MetricsUtil.createRecord(context,"jobs");
        writerMetrics = MetricsUtil.createRecord(context,"writer");
        poolMetrics = MetricsUtil.createRecord(context,"connectionPool");
        context.registerUpdater(this);

    }

    @Override
    public void doUpdates(MetricsContext context) {
        synchronized (this){
            //Get current view of the Task Scheduler
            TaskSchedulerManagement taskManagement = SpliceDriver.driver().getTaskSchedulerManagement();
            numPendingTasks.set(taskManagement.getNumPendingTasks());
            numRunningTasks.set(taskManagement.getNumRunningTasks());
            currentTaskWorkers.set(taskManagement.getCurrentWorkers());
            maxTaskWorkers.set(taskManagement.getCurrentWorkers());

            totalSubmittedTasks.set(taskManagement.getTotalSubmittedTasks());
            totalCompletedTasks.set(taskManagement.getTotalCompletedTasks());
            totalFailedTasks.set(taskManagement.getTotalFailedTasks());
            totalInvalidatedTasks.set(taskManagement.getTotalInvalidatedTasks());
            totalCancelledTasks.set(taskManagement.getTotalCancelledTasks());

            numPendingTasks.pushMetric(this.taskMetrics);
            numRunningTasks.pushMetric(this.taskMetrics);
            currentTaskWorkers.pushMetric(this.taskMetrics);
            maxTaskWorkers.pushMetric(this.taskMetrics);
            totalSubmittedTasks.pushMetric(this.taskMetrics);
            totalCompletedTasks.pushMetric(this.taskMetrics);
            totalFailedTasks.pushMetric(this.taskMetrics);
            totalInvalidatedTasks.pushMetric(this.taskMetrics);
            totalCancelledTasks.pushMetric(this.taskMetrics);

            //get current view of the Job Scheduler
            JobSchedulerManagement jobManagement = SpliceDriver.driver().getJobSchedulerManagement();
            numRunningJobs.set(jobManagement.getNumRunningJobs());
            totalSubmittedJobs.set(jobManagement.getTotalSubmittedJobs());
            totalCompletedJobs.set(jobManagement.getTotalSubmittedJobs());
            totalFailedJobs.set(jobManagement.getTotalSubmittedJobs());
            totalCancelledJobs.set(jobManagement.getTotalSubmittedJobs());

            numRunningJobs.pushMetric(this.jobMetrics);
            totalSubmittedJobs.pushMetric(this.jobMetrics);
            totalCancelledJobs.pushMetric(this.jobMetrics);
            totalFailedJobs.pushMetric(this.jobMetrics);
            totalCompletedJobs.pushMetric(this.jobMetrics);

            /*get TableWriter statistics*/
            TableWriter writer = SpliceDriver.driver().getTableWriter();
            maxBufferHeapSizeWriter.set(writer.getMaxBufferHeapSize());
            maxBufferEntriesWriter.set(writer.getMaxBufferEntries());
            maxFlushesPerBufferWriter.set(writer.getMaxFlushesPerBuffer());
            outstandingCallBuffersWriter.set(writer.getOutstandingCallBuffers());
            pendingBufferFlushesWriter.set(writer.getPendingBufferFlushes());
            executingBufferFlushesWriter.set(writer.getExecutingBufferFlushes());
            totalBufferFlushesWriter.set(writer.getTotalBufferFlushes());
            runningWriteThreadsWriter.set(writer.getRunningWriteThreads());
            cachedTablesWriter.set(writer.getNumCachedTables());
            cacheLastUpdatedWriter.set(writer.getCacheLastUpdatedTimeStamp());
            compressedWritesWriter.set(writer.getCompressWrites()?1:0);

            maxBufferHeapSizeWriter.pushMetric(this.writerMetrics);
            maxBufferEntriesWriter.pushMetric(this.writerMetrics);
            maxFlushesPerBufferWriter.pushMetric(this.writerMetrics);
            outstandingCallBuffersWriter.pushMetric(this.writerMetrics);
            pendingBufferFlushesWriter.pushMetric(this.writerMetrics);
            executingBufferFlushesWriter.pushMetric(this.writerMetrics);
            totalBufferFlushesWriter.pushMetric(this.writerMetrics);
            runningWriteThreadsWriter.pushMetric(this.writerMetrics);
            cachedTablesWriter.pushMetric(this.writerMetrics);
            cacheLastUpdatedWriter.pushMetric(this.writerMetrics);
            compressedWritesWriter.pushMetric(this.writerMetrics);

            /*Get connection Pool statistics*/
            ConnectionPool pool = SpliceDriver.driver().embedConnPool();
            poolWaiting.set(pool.getWaiting());
            poolAvailable.set(pool.getAvailable());
            poolInUse.set(pool.getInUse());
            maxPoolSize.set(pool.getMaxPoolSize());

            poolWaiting.pushMetric(this.poolMetrics);
            poolAvailable.pushMetric(this.poolMetrics);
            poolInUse.pushMetric(this.poolMetrics);
            maxPoolSize.pushMetric(this.poolMetrics);
        }

        this.taskMetrics.update();
        this.jobMetrics.update();
        this.writerMetrics.update();
        this.poolMetrics.update();
    }
}
