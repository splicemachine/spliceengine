package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Predicate;
import com.splicemachine.job.Task;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 12/4/13
 */
public abstract class RegistryConstraint<T extends Task> implements ConstrainedTaskScheduler.Constraint<T> {
		protected final ConcurrentSkipListSet<T> checkedTasks;

		protected RegistryConstraint(Comparator<T> sortOrder) {
				this.checkedTasks = new ConcurrentSkipListSet<T>(sortOrder);
		}

		@Override
		public ConstrainedTaskScheduler.ConstraintAction evaluate(T task) {
				ConstrainedTaskScheduler.ConstraintAction action = doEvaluate(task);
				if(action== ConstrainedTaskScheduler.ConstraintAction.REJECT)
						return action;

				checkedTasks.add(task);
				return action;
		}

		protected abstract ConstrainedTaskScheduler.ConstraintAction doEvaluate(T task);

		protected abstract void recheck(T nextTask) throws ExecutionException;

		protected abstract boolean isRelated(T task, T nextTask);

		@Override
		public boolean complete(T task) throws ExecutionException {
				checkedTasks.remove(task);
				T nextTask = task;
				T t;
				do{
						t = nextTask;
						nextTask = checkedTasks.higher(t);
						if(nextTask!=null && isRelated(task,nextTask)){
								ConstrainedTaskScheduler.ConstraintAction action = doEvaluate(task);
								if(action!= ConstrainedTaskScheduler.ConstraintAction.DEFERRED){
										if(checkedTasks.remove(nextTask)){
												recheck(nextTask);
												return true;
										}
								}
						}
				}while(nextTask!=null);

				//we couldn't find anything above us, so check below
				nextTask = task;
				do{
						t = nextTask;
						nextTask = checkedTasks.lower(t);
						if(nextTask!=null && isRelated(task,nextTask)){
								ConstrainedTaskScheduler.ConstraintAction action = doEvaluate(task);
								if(action!= ConstrainedTaskScheduler.ConstraintAction.DEFERRED){
										if(checkedTasks.remove(nextTask)){
											recheck(nextTask);
											return true;
										}
								}
						}
				}while(nextTask!=null);

				//we couldn't find anything below us either, so we didn't do anything
				return false;
		}

		public T anyDeferredTask(Predicate<T> matcher){

				//find the first deferred task
				for(T t: checkedTasks){
						if(!matcher.apply(t))
								continue;
						if(doEvaluate(t)== ConstrainedTaskScheduler.ConstraintAction.DEFERRED &&
										checkedTasks.remove(t)) {
								return t;
						}
				}
				return null;
		}
}
