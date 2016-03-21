package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.scheduler.RegionTaskControl;
import com.splicemachine.derby.utils.ImportAdmin;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStatusLogger;
import com.splicemachine.job.TaskFuture;

/**
 * This class is a hook for status changes in import tasks for an import job.
 * The 'success' hook is used to write to the import status log when a file has finished an import.
 * There is also a thread that is spawned from this class that intermittently writes the status (# of rows imported and rejected)
 * for each import task to the import status log.  All of these make up the 'progress indicator' for import.
 *
 * @author dwinters
 * @see JobFuture.StatusHook
 */
public class ImportJobInfo extends JobInfo {
	private static final Logger LOG = Logger.getLogger(ImportJobInfo.class);
	private ImportAdmin jobImportAdmin = null;
	private JobStatusLogger jobStatusLogger = null;
	private TaskStatusLoggerThread taskStatusThread = null;
	private ImportContext importContext = null;

	/**
	 * Constructor that starts the task status thread if a job status logger is passed.
	 *
	 * @param jobId
	 * @param jobFuture
	 * @param jobStartMs
	 * @param jobStatusLogger
	 */
	public ImportJobInfo(String jobId, JobFuture jobFuture, long jobStartMs, JobStatusLogger jobStatusLogger, ImportContext importContext) {
		super(jobId, (jobFuture == null ? 0 : jobFuture.getNumTasks()), jobStartMs);
		this.jobFuture = jobFuture;
		this.jobStatusLogger = jobStatusLogger;
		this.importContext = importContext;

		// Only start the task status thread if there is a job status logger.
		if (this.jobStatusLogger != null) {
			this.taskStatusThread = new TaskStatusLoggerThread(/* this */);
			this.taskStatusThread.start();  // Start polling for status of the tasks.
		}

		try {
			this.jobImportAdmin = new ImportAdmin();
		} catch (IOException | SQLException e) {
			LOG.error("Creating ImportAdmin has experienced an exception.", e);
			if (jobStatusLogger != null) {
				jobStatusLogger.log("Creating ImportAdmin has experienced an exception.  Check the logs for the stack trace.");
			}
		}
	}

	/**
	 * Deconstructor to clean up any running threads, etc.
	 */
	public void cleanup() {
		if (taskStatusThread != null) {
			taskStatusThread.requestStop();  // Stop the task status logging thread.
		}
        if (jobImportAdmin != null)
            jobImportAdmin.close();
	}

	/**
	 * This a hook that is called when a task has finished successfully.
	 * Write the status of the tasks to the log and clean up the statistics from JMX for the completed task.
	 *
	 * @param taskFuture the task that completed successfully
	 */
	@Override
	public void success(TaskFuture taskFuture) {
        // Get these now b/c the super call nulls jobFuture
        int numTasks = jobFuture.getNumTasks();
        int remainingTasks = jobFuture.getRemainingTasks();
		super.success(taskFuture);
        if (LOG.isDebugEnabled())
            LOG.debug("Import task succeeded.");
		try {
			// Skip the zero rows since we are reporting the status of a completed task and a new one may overlap briefly.
			logStatusOfImportTasks(jobImportAdmin, true);

			// Clean up our JMX stats for this task.  Remove entries for the taskPath.
			if (taskFuture != null && taskFuture instanceof RegionTaskControl) {
				String taskPath = ((RegionTaskControl)taskFuture).getTaskNode();
				if (taskPath != null) {
					ImportTaskManagementStats.getInstance().cleanup(taskPath);
				}
			}
		} catch (SQLException e) {
			LOG.error("Logging status of the import tasks has experienced an exception.", e);
			if (jobStatusLogger != null) {
				jobStatusLogger.log("Logging status of the import tasks has experienced an exception.  Check the logs for the stack trace.");
			}
		}
		logStatusOfImportFiles(numTasks, remainingTasks);
	}

    @Override
    public void failure(TaskFuture taskFuture) {
        if (LOG.isDebugEnabled())
            LOG.debug("Import task failed.");
        super.failure(taskFuture);
    }

    @Override
    public void cancelled(TaskFuture taskFuture) {
        if (LOG.isDebugEnabled())
            LOG.debug("Import task cancelled.");
        super.cancelled(taskFuture);
    }

    @Override
    public void invalidated(TaskFuture taskFuture) {
        if (LOG.isDebugEnabled())
            LOG.debug("Import task invalidated.");
        super.invalidated(taskFuture);
    }

    public void failJob() {
        if (LOG.isDebugEnabled())
            LOG.debug("Import job failed.");
        super.failJob();
    }

    public void cancel() throws ExecutionException {
        if (LOG.isDebugEnabled())
            LOG.debug("Import job cancelled.");
        super.cancel();
    }

	/**
	 * Write the status of the import files to the job status log.
	 */
	public void logStatusOfImportFiles(int totalTasks, int remainingTasks) {
		if (jobStatusLogger != null) {
			int completedTasks = totalTasks - remainingTasks;
			String logVerb = getLogVerb();
			jobStatusLogger.log(String.format("%d of %d files %sed (%.0f%% files %sed)",
					completedTasks, totalTasks, logVerb,
					((double)completedTasks/(double)totalTasks)*100.0d, logVerb));
		}
	}

	/**
	 * Write the status of all running import tasks to the job status log.
	 *
	 * @param importAdmin   import admin object to access info regarding all of the import tasks running in the cluster
	 * @param skipZeroRows  if true do not display rows that show zero for both rows imported and rejected
	 *
	 * @throws SQLException
	 */
	private void logStatusOfImportTasks(ImportAdmin importAdmin, boolean skipZeroRows) throws SQLException {
		if (importAdmin != null) {
			List<Pair<String, ImportTaskManagement>> importTaskPairs = importAdmin.getRegionServerImportTaskInfo();
			if (importTaskPairs != null) {
				/*
				 * We are going to first write everything into a buffer and then we will write that to the log.
				 * There are a couple reasons why we do it that.
				 *   1. Don't loop through the import task stats twice to calculate first the # of tasks and then
				 *      to write out the number of rows imported or rejected.  If we loop twice, we stand the chance
				 *      that the total # of tasks in the summary line will differ from the actual # of tasks in
				 *      the detailed lines.  This was happening quite regularly before.
				 *   2. This will reduce the number of writes to HDFS where we are forcing the flush.
				 */

				// Write detailed lines of how many rows have been imported/rejected for each import task.
				// Put these lines in a buffer (list) so that we can iterate through the importTaskPairs once and
				// count the import tasks at the same time.  When we have finished looping through them, we will
				// add a summary line at the beginning of the buffer and write out the buffer to the import status log.

				LinkedList<String> logRows = new LinkedList<String>();  // log lines buffer
				int numImportTasks = 0;
				String logVerb = getLogVerb();

				for (Pair<String, ImportTaskManagement> importTaskPair : importTaskPairs) {
					String regionServer = importTaskPair.getFirst();
					ImportTaskManagement importTask = importTaskPair.getSecond();
					Map<String, Long> importedRowsMap = importTask.getTotalImportedRowsByTaskPath();
					numImportTasks += importedRowsMap.size();
					Map<String, Long> badRowsMap = importTask.getTotalBadRowsByTaskPath();
					Map<String, String> filePathsMap = importTask.getImportFilePathsByTaskPath();
					for (Map.Entry<String, Long> importedRowsMapEntry : importedRowsMap.entrySet()) {
						String importTaskPath = importedRowsMapEntry.getKey();
						Long importRowCount = importedRowsMapEntry.getValue();
						Long badRowCount = badRowsMap.get(importTaskPath);
						if (!skipZeroRows ||
                            (importRowCount != null && importRowCount > 0) ||
                            (badRowCount != null && badRowCount > 0)) {
							String importFilePath = filePathsMap.get(importTaskPath);
							logRows.add(String.format("    %sed%s %,d rows and rejected %,d rows from %s on %s",
                                WordUtils.capitalize(logVerb),
                                (importContext != null && importContext.isCheckScan() ? " OK" : ""),
                                (importRowCount != null ? importRowCount : 0),
                                (badRowCount != null ? badRowCount : 0),
                                importFilePath,
                                regionServer));
						} else {
							numImportTasks--;
						}
					}
				}

				// Write a summary line of how many import tasks are currently running, and then write the detailed lines with their row counts.
				logRows.addFirst(String.format("  %d %s task%s:", numImportTasks, logVerb, (numImportTasks == 1 ? " running" : "s running in parallel")));
				if (jobStatusLogger != null) {
					for (String logRow : logRows) {
						jobStatusLogger.log(logRow);
					}
				}
			}
		}
	}

	/**
	 * Returns the verb, "check" or "import", depending on whether records are being imported from the files or just checked.
	 *
	 * @return the verb, "check" or "import"
	 */
	private String getLogVerb() {
		return importContext.isCheckScan() ? "check" : "import";
	}

	/**
	 * A thread that polls JMX every configured interval to check the import statistics of each task.
	 * If the task has made progress (e.g. imported or rejected rows) since the last time it was checked,
	 * the current statistics for the task will be written to the import log.
	 *
	 * @author dwinters
	 */
	private class TaskStatusLoggerThread extends Thread {

		/**
		 * Number of milliseconds to sleep before checking JMX for progress by the tasks.
		 */
		long sleepMillis = SpliceConstants.importTaskStatusLoggingInterval;

        /**
         * Number of milliseconds to spin before waking up to evaluate status checking interval
         */
        long spinPauseMillis = 50;

		/**
		 * Flag that tells the TaskStatusLoggerThread when it should exit.
		 */
		private volatile boolean runTaskStatusLoggerThread = true;

		/**
		 * Default constructor that sets the name of the thread.
		 */
		public TaskStatusLoggerThread() {
			super("task-status-logger-thread");
		}

		/**
		 * Loop checking JMX for task updates (and sleep) until requested to stop.
		 */
		@Override
		public void run() {
			ImportAdmin threadImportAdmin;
            long currentTime = System.currentTimeMillis();
			try {
				threadImportAdmin = new ImportAdmin();  // This class is not thread safe, so we have our own instance.
				try {

					// Initially sleep a bit to give the import tasks some time to get some work done.
                    sleep(spinPauseMillis);

					while (runTaskStatusLoggerThread && !Thread.currentThread().isInterrupted()) {
                        if (System.currentTimeMillis()-currentTime > sleepMillis) {
                            currentTime = System.currentTimeMillis();
                            logStatusOfImportTasks(threadImportAdmin, false);
                        }
                        sleep(spinPauseMillis);
                    }
				} finally {
					threadImportAdmin.close();
				}
			} catch (InterruptedException e) {
				LOG.error("TaskStatusLoggerThread has been interrupted.", e);
				/*
				 * Set the interrupt flag to notify the caller that we got interrupted.
				 * We should be the end of the road here, but do it anyways.  Maybe someone called the run method directly.
				 */
				Thread.currentThread().interrupt();
			} catch (SQLException e) {
				LOG.error("TaskStatusLoggerThread has experienced a SQL exception.  Stopping thread.", e);
				if (jobStatusLogger != null) {
					jobStatusLogger.log("TaskStatusLoggerThread has experienced a SQL exception.  Stopping thread.  Check the logs for the stack trace.");
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
