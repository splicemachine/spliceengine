package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Predicates;
import com.splicemachine.job.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public class ConstrainedTaskScheduler<T extends Task> implements StealableTaskScheduler<T> {

		private static final Logger LOG = Logger.getLogger(ConstrainedTaskScheduler.class);


		public static enum ConstraintAction {
				EXECUTABLE,
				DEFERRED,
				REJECT
		}

		/**
		 * Represents a Constraint on whether or not a Task
		 * can be submitted, must be deferred, or should be rejected outright.
		 */
		public static interface Constraint<T extends Task>{
				/**
				 * Apply the constraint to the task.
				 * @param task the task to evaluate
				 * @return {@code EXECUTABLE} if the task can be executed,
				 * {@code DEFERRED} if the task can be deferred, or {@code REJECT} if
				 * the task should be rejected outright
				 */
				public ConstraintAction evaluate(T task);

				/**
				 * Indicates that a task has been completed, and action may be taken
				 * to initiate the submission of related tasks which have been deferred,
				 * and to clean up any stored state related to this constraint.
				 *
				 * Note that calling this method does <em>not</em> imply that the task
				 * completed successfully, only that it ceased operation.
				 *
				 * @param task the task that finished working
				 * @return true if an action was taken on a related task.
				 */
				public boolean complete(T task) throws ExecutionException;
		}

		/*fields */
		private final TaskScheduler<T> delegate;
		private final List<Constraint<T>> constraints;
		private final RejectionHandler<T> rejectionHandler;

		private final RegistryConstraint<T> overflowConstraint;

		private final AtomicInteger numRunning = new AtomicInteger(0);

		public ConstrainedTaskScheduler(TaskScheduler<T> delegate,
																		List<Constraint<T>> constraints){
				this(delegate, constraints,TaskScheduler.ExceptionRejectionHandler.<T>instance(),false);
		}

		public ConstrainedTaskScheduler(TaskScheduler<T> delegate,
																		List<Constraint<T>> constraints,
																		boolean enableOverflow){
				this(delegate, constraints,TaskScheduler.ExceptionRejectionHandler.<T>instance(),enableOverflow);
		}

		public ConstrainedTaskScheduler(TaskScheduler<T> delegate,
																		List<Constraint<T>> constraints,
																		RejectionHandler<T> rejectionHandler,boolean enableOverflow) {
				this.delegate = delegate;
				this.constraints = constraints;
				this.rejectionHandler = rejectionHandler;
				this.overflowConstraint = !enableOverflow? null: new TasksPerJobConstraint<T>(this, 1, Predicates.<T>alwaysTrue());
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				for(Constraint<T> constraint:constraints){
					ConstraintAction actionTaken = constraint.evaluate(task);
						switch (actionTaken) {
								case DEFERRED:
										task.getTaskStatus().setStatus(Status.PENDING);
										return new ListeningTaskFuture<T>(task,numRunning.get());
								case REJECT:
										rejectionHandler.rejected(task);
						}
				}
				/*
				 * All constraints think we're fine, so we can submit the task
				 * to the underlying scheduler.
				 */
				task.getTaskStatus().attachListener(new CompletionListener(task));
				numRunning.incrementAndGet();
				return delegate.submit(task);
		}

		@Override
		public T steal() {
				//steal from the overflow queue
				return overflowConstraint.anyDeferredTask(Predicates.<T>alwaysTrue());
		}

		private class CompletionListener implements TaskStatus.StatusListener{
				private final T task;

				private CompletionListener(T task) {
						this.task = task;
				}

				@Override
				public void statusChanged(Status oldStatus,
																	Status newStatus,
																	TaskStatus taskStatus) {
						switch (newStatus) {
								case INVALID:
								case FAILED:
								case COMPLETED:
								case CANCELLED:
										numRunning.decrementAndGet();
										taskStatus.detachListener(this);
										try {
												if(overflowConstraint!=null)
														overflowConstraint.complete(task);
												for(Constraint<T> constraint:constraints){
														if(constraint.complete(task)) return;
												}
										}catch(ExecutionException e){
												LOG.error("Unexpected exception when attempting to adjust deferred tasks",e);
										}
						}
				}
		}
}
