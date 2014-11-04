package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Predicate;
import com.splicemachine.job.Task;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public class TasksPerJobConstraint<T extends Task> extends RegistryConstraint<T>{
		private final ConstrainedTaskScheduler<T> taskScheduler;
		private final Predicate<T> application;
		private final int max;

		public TasksPerJobConstraint(ConstrainedTaskScheduler<T> taskScheduler, int max,Predicate<T> application) {
				super(new Comparator<T>() {
						@Override
						public int compare(T o1, T o2) {
								if(o1==null){
										if(o2==null) return 0;
										return -1;
								}else if(o2==null){
										return 1;
								}

								/*
								 * sort lexicographically on job id, then on task id within the job
								 */
								String jobId1 = o1.getJobId();
								String job2 = o2.getJobId();

								int compare = job2.compareTo(jobId1);
								if(compare!=0) return compare;

								byte[] task1 = o1.getTaskId();
								byte[] task2 = o2.getTaskId();

								return Bytes.compareTo(task1,task2);
						}
				});
				this.taskScheduler = taskScheduler;
				this.application = application;
				this.max = max;
		}

		@Override
		protected ConstrainedTaskScheduler.ConstraintAction doEvaluate(T task) {
				if(task==null) return ConstrainedTaskScheduler.ConstraintAction.REJECT;
				if(!application.apply(task))
						return ConstrainedTaskScheduler.ConstraintAction.EXECUTABLE; //we don't care about this type of task
				int count=0;
				T nextTask = checkedTasks.higher(task);
				while(nextTask!=null){
						if(!isRelated(task,nextTask))
								break;
						count++;
						nextTask = checkedTasks.higher(nextTask);
				}
				nextTask = checkedTasks.lower(task);
				while(nextTask!=null){
						if(!isRelated(task,nextTask))
								break;
						count++;
						nextTask = checkedTasks.lower(nextTask);
				}

				if(count>max)
						return ConstrainedTaskScheduler.ConstraintAction.DEFERRED;

				return ConstrainedTaskScheduler.ConstraintAction.EXECUTABLE;
		}

		@Override
		protected void recheck(T nextTask) throws ExecutionException {
				taskScheduler.submit(nextTask);
		}

		@Override
		protected boolean isRelated(T task, T nextTask) {
				return task.getJobId().equals(nextTask.getJobId());
		}
}
