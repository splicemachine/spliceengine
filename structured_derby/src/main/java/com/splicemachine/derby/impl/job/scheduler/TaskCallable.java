package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.Task;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 11/26/13
 */
public class TaskCallable<T extends Task> implements Callable<Void> {
		private static final Logger WORKER_LOG = Logger.getLogger(TaskCallable.class);
		private final T task;

		public TaskCallable(T task) {
				this.task = task;
		}

		@Override
		public Void call() throws Exception {
				switch (task.getTaskStatus().getStatus()) {
						case INVALID:
								if(WORKER_LOG.isTraceEnabled())
										SpliceLogUtils.trace(WORKER_LOG, "Task %s has been invalidated, cleaning up and skipping", Bytes.toString(task.getTaskId()));
								return null;
						case FAILED:
								if(WORKER_LOG.isTraceEnabled())
										SpliceLogUtils.trace(WORKER_LOG, "Task %s has failed, but was not removed from the queue, removing now and skipping", Bytes.toString(task.getTaskId()));
								return null;
						case COMPLETED:
								if(WORKER_LOG.isTraceEnabled())
										SpliceLogUtils.trace(WORKER_LOG, "Task %s has completed, but was not removed from the queue, removing now and skipping", Bytes.toString(task.getTaskId()));
								return null;
						case CANCELLED:
								if(WORKER_LOG.isTraceEnabled())
										SpliceLogUtils.trace(WORKER_LOG,"task %s has been cancelled, not executing",Bytes.toString(task.getTaskId()));
								return null;
				}

				try{
						SchedulerTracer.traceTaskStart();
						if(WORKER_LOG.isTraceEnabled())
								SpliceLogUtils.trace(WORKER_LOG,"executing task %s",Bytes.toString(task.getTaskId()));
						try{
								task.markStarted();
						}catch(CancellationException ce){
								if(WORKER_LOG.isTraceEnabled())
										SpliceLogUtils.trace(WORKER_LOG,"task %s was cancelled",Bytes.toString(task.getTaskId()));
								return null;
						}
						task.execute();
						if(WORKER_LOG.isTraceEnabled())
								SpliceLogUtils.trace(WORKER_LOG, "task %s finished executing, marking completed", Bytes.toString(task.getTaskId()));
						SchedulerTracer.traceTaskEnd();
						completeTask();
				}catch(ExecutionException ee){
						SpliceLogUtils.error(WORKER_LOG,"task "+ Bytes.toString(task.getTaskId())+" had an unexpected error",ee.getCause());
						try{
								task.markFailed(ee.getCause());
						}catch(ExecutionException failEx){
								SpliceLogUtils.error(WORKER_LOG,"Unable to indicate task failure",failEx.getCause());
						}
				}catch(Throwable t){
						SpliceLogUtils.error(WORKER_LOG, "task " + Bytes.toString(task.getTaskId()) + " had an unexpected error while setting state", t);
						try{
								task.markFailed(t);
						}catch(ExecutionException failEx){
								SpliceLogUtils.error(WORKER_LOG,"Unable to indicate task failure",failEx.getCause());
						}
				}
				return null;
		}

		private void completeTask() throws ExecutionException{
				try {
						task.markCompleted();
				} catch (ExecutionException e) {
						try {
								task.markFailed(e);
						} catch (ExecutionException ee) {
								WORKER_LOG.error("Unable to indicate task failure",ee);
                    /*
                     * TODO -sf-
                     *
                     * It IS possible that we could FORCE the JobScheduler to obtain some information about us--
                     * we could forcibly terminate our ZooKeeper session. This would tell the JobScheduler that
                     * this task failed, and it would then retry it. However, currently, we share the same
                     * ZooKeeper session across all tasks, so killing our session would likely kill all running
                     * tasks simultaneously (which would be bad).
                     *
                     * Something to think about would be pooling up ZK connections and fixing one connection
                     * per task thread. Then we could forcibly expire the session as a means of informing the
                     * JobScheduler about badness happening over here. However, There are problems with this approach--
                     * ZooKeeper only allows so many concurrent connections, for one, and for another we use ZooKeeper
                     * to share transaction ids. What happens if we delete the node? can we still rollback the
                     * child transactions? These are issues to think about before we do this. Still, worth a thought
                     */
								throw ee;
						}
				}
		}

		public int getPriority() {
				return task.getPriority();
		}

		public String getJobId() {
				return task.getJobId();
		}

		public Task getTask() {
				return task;
		}
}
