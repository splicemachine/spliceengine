package com.splicemachine.derby.impl.storage;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.scheduler.JobFutureFromResults;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
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

	private static final SingleScanRowProvider EMPTY_PROVIDER = new SingleScanRowProvider(){
		@Override public boolean hasNext() { return false; }
		@Override public ExecRow next() { return null; }
		@Override public void open() { }
		@Override public void close() { }
		@Override public RowLocation getCurrentRowLocation() { return null; }

        @Override public Scan toScan() { return null; }
		@Override public byte[] getTableName() { return null; }

		@Override public int getModifiedRowCount() { return 0; }
		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			return null;
		}
		@Override
		public String toString() {
			return "EmptyProvider { } ";
		}
		
		
	};

	private RowProviders(){}
	
	public static RowProvider singletonProvider(ExecRow row, SpliceRuntimeContext spliceRuntimeContext){
		return new SingletonRowProvider(row,spliceRuntimeContext);
	}

	public static RowProvider sourceProvider(SpliceOperation source, Logger log, SpliceRuntimeContext spliceRuntimeContext){
		return new SourceRowProvider(source, log, spliceRuntimeContext);
	}

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

    public static RowProvider emptyProvider() {
        return EMPTY_PROVIDER;
    }

    public static JobResults completeAllJobs(List<Pair<JobFuture, JobInfo>> jobs)
            throws StandardException {
        return completeAllJobs(jobs, false);
    }

    public static JobResults completeAllJobs(List<Pair<JobFuture, JobInfo>> jobs,
                                             boolean cancelOnError) throws StandardException {
        JobResults results = null;
        StandardException baseError = null;
        List<JobStats> stats = new LinkedList<JobStats>();

        long start = System.nanoTime();

        for (Pair<JobFuture, JobInfo> job : jobs) {
            try {
                job.getFirst().completeAll(job.getSecond());
                stats.add(job.getFirst().getJobStats());

            } catch (ExecutionException ee) {
                SpliceLogUtils.error(LOG, ee);
                if (job.getSecond() != null) {
                    job.getSecond().failJob();
                }
                baseError = Exceptions.parseException(ee.getCause());
                throw baseError;
            } catch (InterruptedException e) {
                SpliceLogUtils.error(LOG, e);
                if (job.getSecond() != null) {
                    job.getSecond().failJob();
                }
                baseError = Exceptions.parseException(e);
                throw baseError;
            } finally {
                if (job.getFirst() != null) {
                    try {
                        job.getFirst().cleanup();
                    } catch (ExecutionException e) {
                        if (baseError == null)
                            baseError = Exceptions.parseException(e.getCause());
                    }
                }
                if (baseError != null) {
                    if (cancelOnError){
                        cancelAll(jobs);
                    }
                    SpliceLogUtils.logAndThrow(LOG, baseError);
                }
            }
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

    public static void cancelAll(Collection<Pair<JobFuture, JobInfo>> jobs) {
        for (Pair<JobFuture, JobInfo> jobToCancel : jobs) {
            try {
                jobToCancel.getFirst().cancel();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG, "Unable to cancel job", e.getCause());
            }
        }
    }

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
        public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return provider.shuffleRows(instructions);
        }

        @Override
        public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return provider.asyncShuffleRows(instructions);
        }

        @Override
        public JobResults finishShuffle(List<Pair<JobFuture,JobInfo>> jobFuture) throws StandardException {
            return provider.finishShuffle(jobFuture);
        }

        @Override
		public RowLocation getCurrentRowLocation() {
			return provider.getCurrentRowLocation();
		}

		@Override
		public String toString() {
			return String.format("DelegatingRowProvider { provider=%s } ",provider);
		}

	}

	private static class SingletonRowProvider extends SingleScanRowProvider{
		private final ExecRow row;
		private boolean emitted = false;
		
		SingletonRowProvider(ExecRow row, SpliceRuntimeContext spliceRuntimeContext){
			this.row = row;
			this.spliceRuntimeContext = spliceRuntimeContext;
		}

        @Override public void open() { emitted = false; }
		@Override public boolean hasNext() { return !emitted; }

		@Override
		public ExecRow next() {
			if(!hasNext()) throw new NoSuchElementException();
			emitted=true;
			return row;
		}

		@Override public void close() {  } //do nothing
        @Override public RowLocation getCurrentRowLocation() { return null; }
        @Override public Scan toScan() { return null; }
        @Override public byte[] getTableName() { return null; }

		@Override
		public int getModifiedRowCount() {
			return 0;
		}

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			return spliceRuntimeContext;
		}
		@Override
		public String toString() {
			return String.format("SingletonRowProvider { row=%s } ",row);
		}

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
        public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            List<Pair<JobFuture,JobInfo>> firstFutures = firstRowProvider.asyncShuffleRows(instructions);
            List<Pair<JobFuture,JobInfo>> secondFutures = secondRowProvider.asyncShuffleRows(instructions);

            List<Pair<JobFuture,JobInfo>> l = new LinkedList<Pair<JobFuture,JobInfo>>();
            l.addAll(firstFutures);
            l.addAll(secondFutures);
            return l;
        }

        @Override
        public JobResults finishShuffle(List<Pair<JobFuture,JobInfo>> jobFutures) throws StandardException {
            return firstRowProvider.finishShuffle(jobFutures);
        }

        public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return finishShuffle( asyncShuffleRows(instructions) );
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

        private SerialCombinedRowProvider(RowProvider firstRowProvider, RowProvider secondRowProvider) {
            super(firstRowProvider, secondRowProvider);
        }

        @Override
        public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
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

    private static class CombinedJobResults implements JobResults {
        private final JobResults firstJobResults;
        private final JobResults secondJobResults;

        private CombinedJobResults(JobResults firstJobResults, JobResults secondJobResults) {
            this.firstJobResults = firstJobResults;
            this.secondJobResults = secondJobResults;
        }

        @Override
        public JobStats getJobStats() {
            return new CombinedJobStats(firstJobResults.getJobStats(), secondJobResults.getJobStats());
        }

        @Override
        public void cleanup() throws StandardException {
            try {
                firstJobResults.cleanup();
            } finally {
                secondJobResults.cleanup();
            }
        }
    }

    private static class CombinedJobStats implements JobStats{
        private final JobStats firstJobStats;
        private final JobStats secondJobStats;

        private CombinedJobStats(JobStats firstJobStats, JobStats secondJobStats) {
            this.firstJobStats = firstJobStats;
            this.secondJobStats = secondJobStats;
        }

        @Override
        public int getNumTasks() {
            return firstJobStats.getNumTasks()+secondJobStats.getNumTasks();
        }

        @Override
        public long getTotalTime() {
            return firstJobStats.getTotalTime()+secondJobStats.getTotalTime();
        }

        @Override
        public int getNumSubmittedTasks() {
            return firstJobStats.getNumSubmittedTasks()+secondJobStats.getNumSubmittedTasks();
        }

        @Override
        public int getNumCompletedTasks() {
            return firstJobStats.getNumCompletedTasks()+secondJobStats.getNumCompletedTasks();
        }

        @Override
        public int getNumFailedTasks() {
            return firstJobStats.getNumFailedTasks()+secondJobStats.getNumFailedTasks();
        }

        @Override
        public int getNumInvalidatedTasks() {
            return firstJobStats.getNumInvalidatedTasks() + secondJobStats.getNumInvalidatedTasks();
        }

        @Override
        public int getNumCancelledTasks() {
            return firstJobStats.getNumCancelledTasks()+secondJobStats.getNumCancelledTasks();
        }

        @Override
        public List<TaskStats> getTaskStats() {       
        	List<TaskStats> taskStats = new ArrayList<TaskStats>();
        	taskStats.addAll(firstJobStats.getTaskStats());
        	taskStats.addAll(secondJobStats.getTaskStats());
        	return taskStats;
        }

        @Override
        public String getJobName() {
            return "combinedJob["+firstJobStats.getJobName()+","+secondJobStats.getJobName()+"]";
        }

        @Override
        public List<byte[]> getFailedTasks() {
            List<byte[]> failedTasks = Lists.newArrayList(firstJobStats.getFailedTasks());
            failedTasks.addAll(secondJobStats.getFailedTasks());
            return failedTasks;
        }
    }

}
