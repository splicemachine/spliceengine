package com.splicemachine.derby.impl.storage;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

/**
 * Static utility methods for managing RowProviders.
 *
 * This class provides implementations for common, obvious RowProvider situations:
 * empty Providers, providers of a single row, etc. as well as utilities relating to
 * RowProviders.
 */
public class RowProviders {

	private static final SingleScanRowProvider EMPTY_PROVIDER = new SingleScanRowProvider(){
		@Override public boolean hasNext() { return false; }
		@Override public ExecRow next() { return null; }
		@Override public void open() { }
		@Override public void close() { }
		@Override public RowLocation getCurrentRowLocation() { return null; }

        @Override public Scan toScan() { return null; }
		@Override public byte[] getTableName() { return null; }

		@Override public int getModifiedRowCount() { return 0; }
	};

	private RowProviders(){}
	
	public static RowProvider singletonProvider(ExecRow row){
		return new SingletonRowProvider(row);
	}

	public static RowProvider sourceProvider(NoPutResultSet source, Logger log){
		return new SourceRowProvider(source,log);
	}

    public static RowProvider combine(RowProvider first,RowProvider second){
        return new CombinedRowProvider(first,second);
    }

    public static RowProvider emptyProvider() {
        return EMPTY_PROVIDER;
    }

	public static abstract class DelegatingRowProvider implements RowProvider{
		protected final RowProvider provider;

		protected DelegatingRowProvider(RowProvider provider) {
			this.provider = provider;
		}

		@Override public void open() throws StandardException { 
			provider.open(); 
		}
		@Override public void close() { provider.close(); }
//		@Override public Scan toScan() { return provider.toScan(); }
		@Override public byte[] getTableName() { return provider.getTableName(); }
		@Override public int getModifiedRowCount() { return provider.getModifiedRowCount(); }
		@Override public boolean hasNext() throws StandardException { return provider.hasNext(); }

        @Override
        public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return provider.shuffleRows(instructions);
        }

        @Override
		public RowLocation getCurrentRowLocation() {
			return provider.getCurrentRowLocation();
		}

	}

	private static class SingletonRowProvider extends SingleScanRowProvider{
		private final ExecRow row;
		private boolean emitted = false;
		
		SingletonRowProvider(ExecRow row){
			this.row = row;
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

    }

	public static class SourceRowProvider extends SingleScanRowProvider{
		private final NoPutResultSet source;
		private final Logger log;
		//private boolean populated = false;
		private ExecRow nextEntry;

		private SourceRowProvider(NoPutResultSet source, Logger log) {
			this.source = source;
			this.log = log;
		}

		@Override
		public void open() {
			try {
				source.open();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(log,e);
			}
		}

		@Override
		public void close() {
			try {
				source.close();
			} catch (StandardException e) {
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
				nextEntry = source.getNextRowCore();
			} catch (StandardException e) {
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
	}

    private static class CombinedRowProvider implements RowProvider{
        private final RowProvider firstRowProvider;
        private final RowProvider secondRowProvider;
        private boolean onFirst = true;

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
        public void close() {
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
        public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            JobStats firstStats = firstRowProvider.shuffleRows(instructions);
            JobStats secondStats = secondRowProvider.shuffleRows(instructions);
            return new CombinedJobStats(firstStats,secondStats);
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
        public boolean hasNext() throws StandardException {
            if(onFirst){
                if(!firstRowProvider.hasNext()){
                    onFirst=false;
                }else
                    return true;
            }
            return secondRowProvider.hasNext();
        }

        @Override
        public ExecRow next() throws StandardException {
            if(onFirst)
                return firstRowProvider.next();
            else return secondRowProvider.next();
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
        public Map<String, TaskStats> getTaskStats() {
            Map<String,TaskStats> taskStats = Maps.newHashMap(firstJobStats.getTaskStats());
            taskStats.putAll(secondJobStats.getTaskStats());
            return taskStats;
        }

        @Override
        public String getJobName() {
            return "combinedJob["+firstJobStats.getJobName()+","+secondJobStats.getJobName()+"]";
        }

        @Override
        public List<String> getFailedTasks() {
            List<String> failedTasks = Lists.newArrayList(firstJobStats.getFailedTasks());
            failedTasks.addAll(secondJobStats.getFailedTasks());
            return failedTasks;
        }
    }

}
