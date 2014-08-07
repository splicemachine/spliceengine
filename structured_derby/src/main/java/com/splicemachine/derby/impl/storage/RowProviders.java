package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.operation.MultiScanOperationJob;
import com.splicemachine.derby.impl.job.scheduler.JobFutureFromResults;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.job.*;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.MultiStatsView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.splicemachine.stats.Metrics;

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
												SpliceLogUtils.logAndThrow(LOG, se);
										}else
												baseError = se;
								}
						}
						if(baseError!=null)
								SpliceLogUtils.logAndThrow(LOG,baseError);
				}finally{
						//perform the intermediate cleanup actions
						Throwable t = null;
						for(Callable<Void> cleanupTask:intermediateCleanupTasks){
								try{
										cleanupTask.call();
								} catch (Exception e) {
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
						SpliceLogUtils.error(LOG, ee);
						if (jobInfo != null) {
								jobInfo.failJob();
						}
						throw Exceptions.parseException(ee.getCause());
				} catch (InterruptedException e) {
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

		public static Pair<JobFuture,JobInfo> submitMultiScanJob(List<Scan> scans, final HTableInterface table,
																														 SpliceObserverInstructions instructions,
																														 SpliceRuntimeContext runtimeContext) throws StandardException {
				instructions.setSpliceRuntimeContext(runtimeContext);
				JobFuture jobFuture = null;
				JobInfo info = null;
				StatementInfo stmtInfo = instructions.getSpliceRuntimeContext().getStatementInfo();
				try {
						long startTimeMs = System.currentTimeMillis();
						MultiScanOperationJob job = getMultiJob(table,instructions,scans);
						jobFuture = SpliceDriver.driver().getJobScheduler().submit(job);
						info = new JobInfo(job.getJobId(), jobFuture.getNumTasks(), startTimeMs);
						info.setJobFuture(jobFuture);
						info.tasksRunning(jobFuture.getAllTaskIds());
						if(stmtInfo!=null){
								stmtInfo.addRunningJob(Bytes.toLong(instructions.getTopOperation().getUniqueSequenceID()),info);
								jobFuture.addCleanupTask(StatementInfo.completeOnClose(stmtInfo, info));
						}
						//TODO -sf- close this earlier
						jobFuture.addIntermediateCleanupTask(new Callable<Void>() {
								@Override
								public Void call() throws Exception {
										table.close();
										return null;
								}
						});

						return Pair.newPair(jobFuture, info);

				} catch (ExecutionException e) {
						LOG.error(e);
						if (info != null){
								info.failJob();
						}
						if (jobFuture != null){
								try {
										jobFuture.cleanup();
								} catch (ExecutionException ee){
										LOG.error("Error cleaning up Scan Job future", ee);
								}
						}
						throw Exceptions.parseException(e.getCause());
				}

		}
		public static Pair<JobFuture,JobInfo> submitScanJob(Scan scan, HTableInterface table,
																												SpliceObserverInstructions instructions,
																												SpliceRuntimeContext runtimeContext) throws StandardException {
				return submitMultiScanJob(Collections.singletonList(scan),table,instructions, runtimeContext);
		}

		private static MultiScanOperationJob getMultiJob(HTableInterface table, SpliceObserverInstructions instructions,List<Scan> scans){
				for(Scan scan:scans){
						if (scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS) == null)
								SpliceUtils.setInstructions(scan, instructions);
				}
				boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
//				StatementInfo info = instructions.getSpliceRuntimeContext().getStatementInfo();

//				long statementId = info!=null? info.getStatementUuid(): -1l;
//				Activation activation = instructions.getTopOperation().getActivation();
//				LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
				return new MultiScanOperationJob(scans, instructions, table, SpliceConstants.operationTaskPriority,readOnly);
		}

//		private static OperationJob getJob(HTableInterface table, SpliceObserverInstructions instructions, Scan scan) {
//				if (scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS) == null)
//						SpliceUtils.setInstructions(scan, instructions);
//				boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
//				StatementInfo info = instructions.getSpliceRuntimeContext().getStatementInfo();
//
//				long statementId = info!=null? info.getStatementUuid(): -1l;
//				Activation activation = instructions.getTopOperation().getActivation();
//				LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
//				return new OperationJob(scan, instructions, table, readOnly,lcc.getRunTimeStatisticsMode(),statementId,lcc.getXplainSchema());
//		}

		public static abstract class DelegatingRowProvider implements RowProvider{

				protected final RowProvider provider;

				protected DelegatingRowProvider(RowProvider provider) {
						this.provider = provider;
				}

				@Override public void open() throws StandardException {
						provider.open();
				}
				@Override public void close() throws StandardException { provider.close(); }
				//		@Override public Scan toScan() { return provider.toScan(); }
				@Override public byte[] getTableName() { return provider.getTableName(); }
				@Override public int getModifiedRowCount() { return provider.getModifiedRowCount(); }
				@Override public boolean hasNext() throws StandardException, IOException { return provider.hasNext(); }

				@Override
				public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
						return provider.shuffleRows(instructions,postCompleteTasks);

				}

				@Override
				public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
						return provider.asyncShuffleRows(instructions);
				}

				@Override
				public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture, Callable<Void>... intermediateCleanupTasks) throws StandardException {
						return provider.finishShuffle(jobFuture,intermediateCleanupTasks);
				}

				@Override public RowLocation getCurrentRowLocation() { return provider.getCurrentRowLocation(); }

				@Override public String toString() { return String.format("DelegatingRowProvider { provider=%s } ",provider); }

				@Override
				public void reportStats(long statementId, long operationId, long taskId, String xplainSchema, String regionName) {
					provider.reportStats(statementId,operationId,taskId,xplainSchema,regionName);
				}
				@Override public IOStats getIOStats() { return provider.getIOStats(); }
		}

		public static class SourceRowProvider extends SingleScanRowProvider{
				private final SpliceOperation source;
				private final Logger log;
				//private boolean populated = false;
				private ExecRow nextEntry;

				private SourceRowProvider(SpliceOperation source, Logger log, SpliceRuntimeContext spliceRuntimeContext) {
						this.source = source;
						this.log = log;
						this.spliceRuntimeContext = spliceRuntimeContext;
				}

				@Override
				public void open() {
						try {
								source.open();
						} catch (StandardException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						} catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						}
				}

				@Override
				public void close() {
						try {
								source.close();
						} catch (StandardException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						} catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						}
				}

				@Override
				public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
						spliceRuntimeContext.setCurrentTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
						SpliceOperation op = instructions.getTopOperation();
						op.init(SpliceOperationContext.newContext(op.getActivation()));
						try {
								OperationSink opSink = OperationSink.create((SinkingOperation) op, null, instructions.getTransactionId(),
												spliceRuntimeContext.getStatementInfo().getStatementUuid(),0l);

								JobStats stats;
								if (op instanceof DMLWriteOperation)
										stats = new LocalTaskJobStats(opSink.sink(((DMLWriteOperation) op).getDestinationTable(), spliceRuntimeContext));
								else {
										byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
										stats = new LocalTaskJobStats(opSink.sink(tempTableBytes, spliceRuntimeContext));
								}
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
				@Override public byte[] getTableName() { return null; }

				@Override
				public int getModifiedRowCount() {
						return source.modifiedRowCount();
				}

				@Override
				public boolean hasNext() {
						//if(populated==true) return true;
						try {
								nextEntry = source.nextRow(spliceRuntimeContext);
						} catch (StandardException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						} catch (IOException e) {
								SpliceLogUtils.logAndThrowRuntime(log,e);
						}
						return nextEntry!=null;
				}

				@Override
				public ExecRow next() {
						//if(!populated) return null;
						//populated=false;
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

				@Override
				public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) {
						if(taskId==-1l) taskId = SpliceDriver.driver().getUUIDGenerator().nextUUID();
						List<OperationRuntimeStats> opStats = OperationRuntimeStats.getOperationStats(
										source, taskId, statementId, WriteStats.NOOP_WRITE_STATS, Metrics.noOpTimeView(), spliceRuntimeContext);
						XplainTaskReporter taskReporter = SpliceDriver.driver().getTaskReporter();
						String hostName = SpliceUtils.getHostName();
						for(OperationRuntimeStats opStat:opStats){
								opStat.setHostName(hostName);
								taskReporter.report(opStat);
						}
				}

				@Override
				public IOStats getIOStats() {
						return Metrics.noOpIOStats(); //don't record stats, since the operation will do it itself
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

				@Override
				public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) {
						firstRowProvider.reportStats(statementId,operationId,taskId,xplainSchema,regionName);
						secondRowProvider.reportStats(statementId,operationId,taskId,xplainSchema,regionName);
				}

				@Override
				public IOStats getIOStats() {
						MultiStatsView stats = new MultiStatsView(Metrics.multiTimeView());
						stats.merge(firstRowProvider.getIOStats());
						stats.merge(secondRowProvider.getIOStats());
						return stats;
				}
		}

		/*
		 * RowProvider that combines two row providers. Rows from the two source providers
		 * are shuffled in series (first then second).
		 */
		private static class SerialCombinedRowProvider extends CombinedRowProvider{

				private SerialCombinedRowProvider(RowProvider firstRowProvider, RowProvider secondRowProvider) {
						super(firstRowProvider, secondRowProvider);
				}

				@Override
				public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
						List<Pair<JobFuture, JobInfo>> l = new LinkedList<Pair<JobFuture, JobInfo>>();
						for (RowProvider rp : Arrays.asList(firstRowProvider, secondRowProvider)) {
								l.add(futurePairForResults(rp.finishShuffle(rp.asyncShuffleRows(instructions))));
						}
						return l;
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
