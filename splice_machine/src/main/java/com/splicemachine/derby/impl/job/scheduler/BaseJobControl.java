package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.ExceptionTranslator;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.load.ImportTaskManagement;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ImportAdmin;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStatusLogger;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.exception.AttemptsExhaustedException;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
public abstract class BaseJobControl implements JobFuture {
    private static final Logger LOG = Logger.getLogger(BaseJobControl.class);
    protected final CoprocessorJob job;
    protected final NavigableSet<RegionTaskControl> tasksToWatch;
    protected final BlockingQueue<RegionTaskControl> changedTasks;
    protected final Set<RegionTaskControl> failedTasks;
    protected final Set<RegionTaskControl> completedTasks;
    protected final Set<RegionTaskControl> cancelledTasks;
    protected final JobStatsAccumulator stats;
    protected final SpliceZooKeeperManager zkManager;
    protected final int maxResubmissionAttempts;
    protected final JobMetrics jobMetrics;
    protected final String jobPath;
    protected final JobStatusLogger jobStatusLogger;
    protected final TaskStatusLoggerThread taskStatusThread;
    protected final List<Callable<Void>> finalCleanupTasks;
    protected final List<Callable<Void>> intermediateCleanupTasks;
    protected volatile boolean cancelled = false;
    protected volatile boolean cleanedUp = false;
    protected volatile boolean intermediateCleanedUp = false;

    BaseJobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics, JobStatusLogger jobStatusLogger){
        this.job = job;
        this.jobPath = jobPath;
        this.zkManager = zkManager;
        this.jobMetrics = jobMetrics;
        this.stats = new JobStatsAccumulator(job.getJobId());
        this.tasksToWatch = new ConcurrentSkipListSet<RegionTaskControl>();
        this.jobStatusLogger = jobStatusLogger;
        this.taskStatusThread = new TaskStatusLoggerThread(this);
        this.taskStatusThread.start();  // Start polling for status of the import tasks.

        this.changedTasks = new LinkedBlockingQueue<RegionTaskControl>();
        this.failedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.cancelledTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.finalCleanupTasks = Lists.newLinkedList();
        this.intermediateCleanupTasks = Lists.newLinkedList();
        this.maxResubmissionAttempts = maxResubmissionAttempts;
    }

    @Override
    public Status getStatus() throws ExecutionException {
        if(failedTasks.size()>0) return Status.FAILED;
        else if(cancelled) return Status.CANCELLED;
        else if(completedTasks.size()>=tasksToWatch.size()) return Status.COMPLETED;
        else return Status.EXECUTING;
    }

    @Override
    public void completeAll(StatusHook statusHook) throws ExecutionException, InterruptedException, CancellationException {
        while(getRemainingTasks()>0)
            completeNext(statusHook);
    }

    @Override
    public void completeNext(StatusHook statusHook) throws ExecutionException, InterruptedException, CancellationException {
        if (failedTasks.size() > 0) {
            for (RegionTaskControl taskControl : failedTasks)
                taskControl.complete(); //throw the error right away
        } else if (cancelled)
            throw new CancellationException();

        RegionTaskControl changedFuture;
        int futuresRemaining = getRemainingTasks();
        SpliceLogUtils.trace(LOG, "[%s]Tasks remaining: %d", job.getJobId(), futuresRemaining);
        boolean found;
        while (futuresRemaining > 0) {
            changedFuture = changedTasks.take();
            if (cancelled)
                throw new CancellationException();
            found = !completedTasks.contains(changedFuture) &&
                    !failedTasks.contains(changedFuture) &&
                    !cancelledTasks.contains(changedFuture);

            futuresRemaining = getRemainingTasks();
            if (!found) continue;

            Status status = changedFuture.getStatus();
            switch (status) {
                case INVALID:
                    changedFuture.cleanup();
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is invalid, resubmitting", job.getJobId(), changedFuture.getTaskNode());
                    stats.invalidTaskCount.incrementAndGet();
                    if (statusHook != null)
                        statusHook.invalidated(changedFuture.getTaskId());

                    if (changedFuture.rollback()) {
                        resubmit(changedFuture, changedFuture.tryNumber());
                    } else {
                        //we were unable to roll back, so we have to bomb out
                        failedTasks.add(changedFuture);
                        completeNext(statusHook); //throw the proper error
                    }
                    break;
                case FAILED:
                    if (statusHook != null)
                        statusHook.failure(changedFuture.getTaskId());
                    changedFuture.cleanup();
                    try {
                        SpliceLogUtils.trace(LOG, "[%s] Task %s failed", job.getJobId(), changedFuture.getTaskNode());
                        stats.addFailedTask(changedFuture.getTaskId());
                        changedFuture.dealWithError();
                    } catch (ExecutionException ee) {
                        //update our metrics
                        failedTasks.add(changedFuture);
                        throw ee;
                    }
                    break;
                case COMPLETED:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s completed successfully", job.getJobId(), changedFuture.getTaskNode());
                    changedFuture.cleanup();
                    if (changedFuture.commit()) {
                        TaskStats taskStats = changedFuture.getTaskStats();
                        if (taskStats != null)
                            this.stats.addTaskStatus(changedFuture.getTaskNode(), taskStats);
                        completedTasks.add(changedFuture);
                        if (statusHook != null)
                            statusHook.success(changedFuture.getTaskId());
                        if (jobStatusLogger != null) {
                        	jobStatusLogger.log(String.format("%d of %d files imported...", (getNumTasks() - getRemainingTasks()), getNumTasks()));
                        }
                        return;
                    } else {
                        //our commit failed, we have to resubmit the task (if possible)
                        SpliceLogUtils.debug(LOG, "[%s] Task %s did not successfully commit", job.getJobId(), changedFuture.getTaskNode());
                        if (statusHook != null)
                            statusHook.failure(changedFuture.getTaskId());
                        changedFuture.dealWithError();
                    }
                    break;
                case CANCELLED:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is cancelled", job.getJobId(), changedFuture.getTaskNode());
                    if (statusHook != null)
                        statusHook.cancelled(changedFuture.getTaskId());
                    changedFuture.cleanup();
										changedFuture.rollback();
                    cancelledTasks.add(changedFuture);
                    throw new CancellationException();
                default:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is in state %s", job.getJobId(), changedFuture.getTaskNode(), status);
            }
        }

//        //update our job metrics
//        Status finalStatus = status();
//        jobMetrics.removeJob(job.getJobId(), finalStatus);
//
        SpliceLogUtils.trace(LOG, "completeNext finished");
    }

    @Override
    public void cancel() throws ExecutionException {
				cancelled=true;
				cleanup();
    }

    @Override
    public double getEstimatedCost() throws ExecutionException {
        double maxCost = 0d;
        for(TaskFuture future:tasksToWatch){
            if(maxCost < future.getEstimatedCost()){
                maxCost = future.getEstimatedCost();
            }
        }
        return maxCost;
    }

    @Override
    public void cleanup() throws ExecutionException {
    	if(cleanedUp)
    		return; //don't try cleaning up twice
    	else
    		cleanedUp = true;
    	SpliceLogUtils.trace(LOG, "cleaning up job %s", job.getJobId());
    	intermediateCleanup(); //in case cleanups don't get called
    	try {
    		ZooKeeper zooKeeper = zkManager.getRecoverableZooKeeper().getZooKeeper();
    		zooKeeper.delete(jobPath, -1, new AsyncCallback.VoidCallback() {
    			@Override
    			public void processResult(int i, String s, Object o) {
    				if(LOG.isTraceEnabled())
    					LOG.trace("Result for deleting path " + jobPath + ": i=" + i + ", s=" + s);
    			}
    		}, this);
    	} catch (ZooKeeperConnectionException e) {
    		throw new ExecutionException(e);
    	} finally {
    		try {
    			for(Callable<Void> c: finalCleanupTasks){
    				c.call();
    			}
    		} catch (Exception e) {
    			throw new ExecutionException(e);
    		} finally {
    			finalCleanupTasks.clear();
    			completedTasks.clear();
    			failedTasks.clear();
    			tasksToWatch.clear();
    			changedTasks.clear();
    			taskStatusThread.requestStop();  // Stop the task status logging thread.
    		}
    	}
    }

    @Override
    public void intermediateCleanup() throws ExecutionException {
    	if(intermediateCleanedUp)
    		return; //don't cleanup twice
    	else
    		intermediateCleanedUp = true;

    	ZooKeeper zooKeeper;
    	try {
    		zooKeeper = zkManager.getRecoverableZooKeeper().getZooKeeper();
    	} catch (ZooKeeperConnectionException e) {
    		throw new ExecutionException(e);
    	}
    	for (RegionTaskControl task : tasksToWatch) {
    		zooKeeper.delete(task.getTaskNode(), -1, new AsyncCallback.VoidCallback() {
    			@Override
    			public void processResult(int i, String s, Object o) {
    				if(LOG.isTraceEnabled())
    					LOG.trace("Result for deleting path " + jobPath + ": i=" + i + ", s=" + s);
    			}
    		}, this);
    	}

    	Throwable error = null;
    	for(Callable<Void> c:intermediateCleanupTasks){
    		try {
    			c.call();
    		} catch (Exception e) {
    			error = e;
    		}
    	}
    	if(error!=null)
    		throw new ExecutionException(error);
    }

		@Override
    public void addCleanupTask(Callable<Void> closable) {
        // prepend, so finalCleanupTasks are closed in reverse
        finalCleanupTasks.add(0, closable);
    }

		@Override
		public void addIntermediateCleanupTask(Callable<Void> callable) {
				intermediateCleanupTasks.add(callable);
		}

    @Override public JobStats getJobStats() { return stats; }
    @Override public int getNumTasks() { return tasksToWatch.size(); }

    @Override
    public int getRemainingTasks() {
        return tasksToWatch.size()-completedTasks.size()-failedTasks.size()-cancelledTasks.size();
    }

		@Override
		public byte[][] getAllTaskIds() {
				byte[][] tIds = new byte[tasksToWatch.size()][];
				int i=0;
				for(RegionTaskControl taskControl:tasksToWatch){
						tIds[i] = taskControl.getTaskId();
						i++;
				}
				return tIds;
		}

		/*************************************************************************************************************************/
    /*package local operators*/

    /*
     * Notify the JobControl that an individual task has changed
     */
    void taskChanged(RegionTaskControl taskControl){
        this.changedTasks.add(taskControl);
    }

    /*
     * Notify the JobControl that an individual task has been invalidated
     */
    @SuppressWarnings("UnusedParameters")
    void markInvalid(RegionTaskControl regionTaskControl) {
        stats.invalidTaskCount.incrementAndGet();
    }

    /*
     * Resubmit a task.
     *
     * if tryCount >=maxResubmissionAttempts, then we cannot attempt this task any longer. An AttemptsExhaustedException
     * will be thrown
     */
    void resubmit(RegionTaskControl task,
                  int tryCount) throws ExecutionException {
        Throwable lastError = task.getError();
        ExceptionTranslator exceptionHandler = DerbyFactoryDriver.derbyFactory.getExceptionHandler();
        if(tryCount<maxResubmissionAttempts || (lastError!=null && exceptionHandler.canInfinitelyRetry(lastError))){
            doResumbit(task,tryCount);
        }else{
				    //only submit so many times
            if(lastError!=null)
                throw new ExecutionException(lastError);

            //we don't know what went wrong, so blow up with an AttemptsExhausted
            ExecutionException ee = new ExecutionException(
                    new AttemptsExhaustedException("Unable to complete task "+ task.getTaskNode()+", it was invalidated more than "+ maxResubmissionAttempts+" times"));
            task.fail(ee.getCause());
            throw ee;
        }
    }

    private void doResumbit(RegionTaskControl task, int tryCount) throws ExecutionException {
        //get the next higher task
        RegionTaskControl next = task;
        do{
            next = tasksToWatch.higher(next);
            /*
             * Can't do a direct compareTo() call, because the compareTo() method must
             * distinguish between two regions which have the same start/stop key but are
             * distinct task nodes, where here we want to treat those the same.
             */
        }while(next!=null && Bytes.compareTo(next.getStartRow(), task.getStartRow())==0);

        tasksToWatch.remove(task);

        byte[] endRow;
        byte[] start = task.getStartRow();
        if(next!=null){
            byte[] nextStart = next.getStartRow();
            endRow = new byte[nextStart.length];
            System.arraycopy(nextStart,0,endRow,0,endRow.length);
        }else
            endRow = HConstants.EMPTY_END_ROW;

        try{
            Pair<RegionTask,Pair<byte[],byte[]>> newTaskData = job.resubmitTask(task.getTask(), start, endRow);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "executing submit on resubmitted job %s", job.getJobId());

						TxnView parentTxn = job.getTxn();
            if(parentTxn!=null){
								//set a new transaction on the entry
                newTaskData.getFirst().setParentTxnInformation(parentTxn);
						}
            //submit the task again
            submit(newTaskData.getFirst(), newTaskData.getSecond(), job.getTable(), tryCount + 1);
        }catch(IOException ioe){
            throw new ExecutionException(ioe);
        }
    }

    /*
     * Physically submits a Task.
     */
    public abstract void submit(final RegionTask task,
                        Pair<byte[], byte[]> range,
                        HTableInterface table,
                        final int tryCount) throws ExecutionException;

    /**
     * A thread that polls JMX every configured interval to check the import statistics of each import task.
     * If the task has made progress (e.g. imported or rejected rows) since the last time it was checked,
     * the current statistics for the import task will be written to the import log.
     *
     * @author dwinters
     */
    private class TaskStatusLoggerThread extends Thread {

    	/**
    	 * Reference to parent job control.
    	 */
    	BaseJobControl jobControl = null;

    	/**
    	 * Number of milliseconds to sleep before checking JMX for progress by the import tasks.
    	 */
    	long sleepMillis = SpliceConstants.importTaskStatusLoggingInterval;

    	/**
    	 * Flag that tells the TaskStatusLoggerThread when it should exit.
    	 */
    	private boolean runTaskStatusLoggerThread = true;

    	/**
    	 * Default constructor that sets the name of the thread.
    	 *
    	 * @param jobControl parent job control object
    	 */
    	public TaskStatusLoggerThread(BaseJobControl jobControl) {
    		super("task-status-logger-thread");
    		this.jobControl = jobControl;
    	}

    	/**
    	 * Loop checking JMX for import task updates (and sleep) until requested to stop.
    	 */
    	@Override
    	public void run() {
			ImportAdmin importAdmin;
			try {
				importAdmin = new ImportAdmin();
				try {
					while (runTaskStatusLoggerThread) {

						/*
						 * Write the status of all running import tasks to the import job status log.
						 */
						try {
							List<Pair<String, ImportTaskManagement>> importTaskPairs = importAdmin.getRegionServerImportTaskInfo();
							for (Pair<String, ImportTaskManagement> importTaskPair : importTaskPairs) {
								String regionServer = importTaskPair.getFirst();
								ImportTaskManagement importTask = importTaskPair.getSecond();
								Map<String, Long> importedRowsMap = importTask.getTotalImportedRowsByFilePath();
								Map<String, Long> badRowsMap = importTask.getTotalBadRowsByFilePath();
								for (Map.Entry<String, Long> importedRowsMapEntry : importedRowsMap.entrySet()) {
									String importFileName = importedRowsMapEntry.getKey();
									Long importRowCount = importedRowsMapEntry.getValue();
									Long badRowCount = badRowsMap.get(importFileName);
									if (jobStatusLogger != null) {
										jobStatusLogger.log(String.format("Imported %d rows and rejected %d rows from %s on %s", importRowCount, badRowCount, importFileName, regionServer));
									}
								}
							}
						} catch (SQLException e) {
							LOG.error("TaskStatusLoggerThread has experienced a SQL exception.  Stopping thread.", e);
							if (jobStatusLogger != null) {
								jobStatusLogger.log("TaskStatusLoggerThread has experienced a SQL exception.  Stopping thread.  Check the logs for the stack trace.");
							}
							break;
						}

						/*
						 * Sleep for a configured amount of time and then log the import task status again.
						 */
						try {
							sleep(sleepMillis);
						} catch (InterruptedException e) {
							LOG.error("TaskStatusLoggerThread has been interrupted.", e);
						}
					}
				} finally {
					importAdmin.close();
				}
			} catch (Exception e) {
				LOG.error("TaskStatusLoggerThread has experienced an exception.  Stopping thread.", e);
				if (jobStatusLogger != null) {
					jobStatusLogger.log("TaskStatusLoggerThread has experienced an exception.  Stopping thread.  Check the logs for the stack trace.");
				}
			}
    	}

    	/**
    	 * Request the current thread to stop after it is done sleeping.
    	 */
    	public void requestStop() {
    		runTaskStatusLoggerThread = false;
    	}
    }
}
