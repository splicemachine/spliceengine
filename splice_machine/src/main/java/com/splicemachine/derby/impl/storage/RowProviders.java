package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.scheduler.JobFutureFromResults;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationSink;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSinkFactory;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Static utility methods for managing RowProviders.
 *
 * This class provides implementations for common, obvious RowProvider situations:
 * empty Providers, providers of a single row, etc. as well as utilities relating to
 * RowProviders.
 */
public class RowProviders {

		private static final Logger LOG = Logger.getLogger(RowProviders.class);

		private RowProviders(){}

		public static RowProvider openedSourceProvider(SpliceOperation source, Logger log, SpliceRuntimeContext spliceRuntimeContext){
				return new SourceRowProvider(source, log, spliceRuntimeContext){
						@Override
						public void open() {
								//no-op
						}
                    @Override
                    public void close() {
                        //no-op - see SourceRowProvider class
                    }
				};
		}


		public static RowProvider combine(RowProvider first,RowProvider second){
				return new CombinedRowProvider(first,second);
		}

		public static RowProvider combineInSeries(RowProvider first,RowProvider second){
				return new SerialCombinedRowProvider(first,second);
		}

		@SuppressWarnings("ThrowFromFinallyBlock")
		public static JobResults completeAllJobs(List<Pair<JobFuture, JobInfo>> jobs,
																						 boolean cancelOnError,Callable<Void>...intermediateCleanupTasks) throws StandardException {
				JobResults results = null;
				StandardException baseError = null;
				List<JobStats> stats = new LinkedList<JobStats>();

				long start = System.nanoTime();

				try{
						for (Pair<JobFuture, JobInfo> jobPair : jobs) {
								try{
										stats.add(completeJob(jobPair));
								}catch(StandardException se){
										if(cancelOnError){
												cancelAll(jobs);
                                            SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(se));
												SpliceLogUtils.logAndThrow(LOG, se);
										}else
												baseError = se;
								}
						}
						if(baseError!=null) {
                            SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(baseError));
                            SpliceLogUtils.logAndThrow(LOG, baseError);
                        }
				}finally{
						//perform the intermediate cleanup actions
						Throwable t = null;
						for(Callable<Void> cleanupTask:intermediateCleanupTasks){
								try{
										// DB-2444: skip the sub operation job cleanup for now.
									    // All the operations will still get cleaned up for a query
									    // by the topOperation.close() invocation from SpliceNoPutResultSet.
										//
										// cleanupTask.call();
								} catch (Exception e) {
                                    SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(e));
										if(t==null)
												t = e;
								}
						}
						if(t!=null)
								throw Exceptions.parseException(t);
				}

				long end = System.nanoTime();

				if (jobs.size() > 1) {
						results = new CompositeJobResults(Lists.transform(jobs, new Function<Pair<JobFuture, JobInfo>, JobFuture>() {
								@Override
								public JobFuture apply(Pair<JobFuture, JobInfo> job) {
										return job.getFirst();
								}
						}), stats, end - start);
				} else if (jobs.size() == 1) {
						results = new SimpleJobResults(stats.iterator().next(), jobs.get(0).getFirst());
				}

				return results;
		}

		@SuppressWarnings("ThrowFromFinallyBlock")
		protected static JobStats completeJob(Pair<JobFuture, JobInfo> jobPair) throws StandardException {
				JobFuture job = jobPair.getFirst();
				JobInfo jobInfo = jobPair.getSecond();
				try {
						job.completeAll(jobInfo);
						return job.getJobStats();
				} catch (ExecutionException ee) {
                        SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(ee));
						SpliceLogUtils.error(LOG, ee);
						if (jobInfo != null) {
								jobInfo.failJob();
						}
						throw Exceptions.parseException(ee.getCause());
				} catch (InterruptedException e) {
                        SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(e));
						SpliceLogUtils.error(LOG, e);
						if (jobInfo != null) {
								jobInfo.failJob();
						}
						throw Exceptions.parseException(e);
				} finally {
						if (job != null) {
								try {
										job.intermediateCleanup();
								} catch (ExecutionException e) {
										throw Exceptions.parseException(e);
								}
						}
				}
		}

		public static void cancelAll(Collection<Pair<JobFuture, JobInfo>> jobs) {
				for (Pair<JobFuture, JobInfo> jobToCancel : jobs) {
						try {
								jobToCancel.getFirst().cancel();
						} catch (ExecutionException e) {
								SpliceLogUtils.error(LOG, "Unable to cancel job", e.getCause());
						}
				}
		}

    /**
     * A RowProvider that implements all methods by delegating to the passed RowProvider.  Extending is a
     * convenient way to change the behavior of just a few methods of the delegate without proxying.
     */
    public static abstract class DelegatingRowProvider implements RowProvider {

        protected final RowProvider provider;

        protected DelegatingRowProvider(RowProvider provider) {
            this.provider = provider;
        }

        @Override
        public void open() throws StandardException {
            provider.open();
        }

        @Override
        public void close() throws StandardException {
            provider.close();
        }

        @Override
        public byte[] getTableName() {
            return provider.getTableName();
        }

        @Override
        public int getModifiedRowCount() {
            return provider.getModifiedRowCount();
        }

        @Override
        public boolean hasNext() throws StandardException, IOException {
            return provider.hasNext();
        }

        @Override
        public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
            return provider.shuffleRows(instructions, postCompleteTasks);
        }

        @Override
        public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
            return provider.asyncShuffleRows(instructions);
        }

        @Override
        public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture, Callable<Void>... intermediateCleanupTasks) throws StandardException {
            return provider.finishShuffle(jobFuture, intermediateCleanupTasks);
        }

        @Override
        public RowLocation getCurrentRowLocation() {
            return provider.getCurrentRowLocation();
        }

        @Override
        public String toString() {
            return String.format("DelegatingRowProvider { provider=%s } ", provider);
        }

        @Override
        public SpliceRuntimeContext getSpliceRuntimeContext() {
            return provider.getSpliceRuntimeContext();
        }

    }

		public static abstract class SourceRowProvider extends SingleScanRowProvider{
				private final SpliceOperation source;
				private final Logger log;
				private ExecRow nextEntry;
                private String tableName;

				private SourceRowProvider(SpliceOperation source, Logger log, SpliceRuntimeContext spliceRuntimeContext) {
						this.source = source;
						this.log = log;
						this.spliceRuntimeContext = spliceRuntimeContext;
                        this.tableName = new String(spliceRuntimeContext.getTempTable().getTempTableName());
				}

				@Override
				public abstract void open();

				@Override
				public abstract void close();

				@Override
				public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
						spliceRuntimeContext.setCurrentTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
						SpliceOperation op = instructions.getTopOperation();
						op.init(SpliceOperationContext.newContext(op.getActivation()));
						try {
								OperationSink opSink = OperationSinkFactory.create((SinkingOperation) op, null, instructions.getTxn(),
                                        spliceRuntimeContext.getStatementInfo().getStatementUuid(), 0L);
                            TaskStats taskStats = opSink.sink(spliceRuntimeContext);
                            JobStats stats = new LocalTaskJobStats(taskStats);
								for(Callable<Void> callable:postCompleteTasks){
										try{
												callable.call();
										}catch(Exception e){

												throw Exceptions.parseException(e);
										}
								}

								return new SimpleJobResults(stats, null);
						} catch (Exception e) {
								throw Exceptions.parseException(e);
						}
				}

				@Override
				public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions)
								throws StandardException, IOException {
						return Collections.singletonList(futurePairForResults(shuffleRows(instructions)));
				}

				@Override public RowLocation getCurrentRowLocation() { return null; }
				@Override public Scan toScan() { return null; }
				@Override public byte[] getTableName() { //{ return null; }
                    return tableName.getBytes();
                }

				@Override
				public int getModifiedRowCount() {
						return source.modifiedRowCount();
				}

				@Override
				public boolean hasNext() {
						try {
								nextEntry = source.getNextRow();
						} catch (StandardException e) {
                            SpliceLogUtils.error(LOG, org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(e));
								SpliceLogUtils.logAndThrowRuntime(log,e);
						}
                    return nextEntry!=null;
				}

				@Override
				public ExecRow next() {
						return nextEntry;
				}

				@Override
				public SpliceRuntimeContext getSpliceRuntimeContext() {
						return spliceRuntimeContext;
				}

				@Override
				public String toString() {
						return String.format("SourceRowProvider { source=%s } ",source);
				}

		}

		/*
		 * RowProvider that combines two row providers. Rows from the two source providers
		 * are shuffled in parallel.
		 */
		private static class CombinedRowProvider implements RowProvider{
				protected final RowProvider firstRowProvider;
				protected final RowProvider secondRowProvider;
				protected boolean onFirst = true;

				private CombinedRowProvider(RowProvider firstRowProvider, RowProvider secondRowProvider) {
						this.firstRowProvider = firstRowProvider;
						this.secondRowProvider = secondRowProvider;
				}

				@Override
				public void open() throws StandardException {
						firstRowProvider.open();
						secondRowProvider.open();
				}

				@Override
				public void close() throws StandardException {
						//guarantee that both get close() called to avoid leaks
						try{
								firstRowProvider.close();
						}finally{
								secondRowProvider.close();
						}
				}

				@Override
				public RowLocation getCurrentRowLocation() {
						if(onFirst)
								return firstRowProvider.getCurrentRowLocation();
						else return secondRowProvider.getCurrentRowLocation();
				}

				@Override
				public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
						List<Pair<JobFuture,JobInfo>> firstFutures = firstRowProvider.asyncShuffleRows(instructions);
						List<Pair<JobFuture,JobInfo>> secondFutures = secondRowProvider.asyncShuffleRows(instructions);

						List<Pair<JobFuture,JobInfo>> l = new LinkedList<Pair<JobFuture,JobInfo>>();
						l.addAll(firstFutures);
						l.addAll(secondFutures);
						return l;
				}

				@Override
				public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFutures, Callable<Void>... intermediateCleanupTasks) throws StandardException {
						return firstRowProvider.finishShuffle(jobFutures,intermediateCleanupTasks);
				}

				public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
						return finishShuffle(asyncShuffleRows(instructions),postCompleteTasks);
				}

				@Override
				public byte[] getTableName() {
						if(onFirst) return firstRowProvider.getTableName();
						else return secondRowProvider.getTableName();
				}

				@Override
				public int getModifiedRowCount() {
						return firstRowProvider.getModifiedRowCount()+secondRowProvider.getModifiedRowCount();
				}

				@Override
				public boolean hasNext() throws StandardException, IOException {
						if(onFirst){
								if(!firstRowProvider.hasNext()){
										onFirst=false;
								}else
										return true;
						}
						return secondRowProvider.hasNext();
				}

				@Override
				public ExecRow next() throws StandardException, IOException {
						if(onFirst)
								return firstRowProvider.next();
						else return secondRowProvider.next();
				}

				@Override
				public SpliceRuntimeContext getSpliceRuntimeContext() {
						return null;
				}
				@Override
				public String toString() {
						return String.format("CombinedRowProvider { firstRowProvider=%s, secondRowProvider=%s } ",firstRowProvider, secondRowProvider);
				}

		}

		/*
		 * RowProvider that combines two row providers. Rows from the two source providers
		 * are shuffled in series (first then second).
		 */
		private static class SerialCombinedRowProvider extends CombinedRowProvider{
                private JobResults firstJobResults;

				private SerialCombinedRowProvider(RowProvider firstRowProvider, RowProvider secondRowProvider) {
						super(firstRowProvider, secondRowProvider);
				}

            public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFutures, Callable<Void>... intermediateCleanupTasks) throws StandardException {
                JobResults jobResults = super.finishShuffle(jobFutures, intermediateCleanupTasks);
                if (firstJobResults != null)
                    firstJobResults.cleanup();
                return jobResults;
            }

				@Override
				public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
                    firstJobResults = firstRowProvider.finishShuffle(firstRowProvider.asyncShuffleRows(instructions));
                    return secondRowProvider.asyncShuffleRows(instructions);
                }

				@Override
				public String toString() {
						return String.format("SerialCombinedRowProvider { firstRowProvider=%s, secondRowProvider=%s } ", firstRowProvider, secondRowProvider);
				}

		}

		public static Pair<JobFuture,JobInfo> futurePairForResults(JobResults results){
				JobFuture future = new JobFutureFromResults(results);
				JobInfo info = new JobInfo(results.getJobStats().getJobName(), future.getNumTasks());
				info.setJobFuture(future);
				return Pair.newPair(future, info);
		}

		private static class LocalTaskJobStats implements JobStats {
				private final TaskStats stats;

				public LocalTaskJobStats(TaskStats stats) {
						this.stats = stats;
				}

				@Override
				public int getNumTasks() {
						return 1;
				}

				@Override
				public int getNumSubmittedTasks() {
						return 1;
				}

				@Override
				public int getNumCompletedTasks() {
						return 1;
				}

				@Override
				public int getNumFailedTasks() {
						return 0;
				}

				@Override
				public int getNumInvalidatedTasks() {
						return 0;
				}

				@Override
				public int getNumCancelledTasks() {
						return 0;
				}

				@Override
				public long getTotalTime() {
						return stats.getTotalTime();
				}

				@Override
				public String getJobName() {
						return "localJob";
				}

				@Override
				public List<byte[]> getFailedTasks() {
						return Collections.emptyList();
				}

				@Override
				public List<TaskStats> getTaskStats() {
						return Arrays.asList(stats);
				}
		}
}
