package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskStatus;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 11/27/13
 */
public class ListeningTaskFuture<T extends Task> implements TaskFuture,TaskStatus.StatusListener {
		private final T task;
		private final double cost;
		public ListeningTaskFuture(T task, double cost) {
				this.task = task;
				this.cost = cost;
		}

		@Override
		public Status getStatus() throws ExecutionException {
				return task.getTaskStatus().getStatus();
		}

		@Override
		public void complete() throws ExecutionException, CancellationException, InterruptedException {
				TaskStatus status = task.getTaskStatus();
				while(true){
						synchronized (this){
								switch (status.getStatus()) {
										case CANCELLED:
												throw new CancellationException();
										case FAILED:
												throw new ExecutionException(status.getError());
										case INVALID:
										case COMPLETED:
												return;
										default:
												wait();
								}
						}
				}
		}

		@Override
		public double getEstimatedCost() {
				return cost;
		}

		@Override
		public byte[] getTaskId() {
				return task.getTaskId();
		}

		@Override
		public TaskStats getTaskStats() {
				return task.getTaskStatus().getStats();
		}

		@Override
		public void statusChanged(Status oldStatus, Status newStatus, TaskStatus taskStatus) {
				synchronized (this){
						notifyAll();
				}
		}

		public T getTask() {
				return task;
		}
}
