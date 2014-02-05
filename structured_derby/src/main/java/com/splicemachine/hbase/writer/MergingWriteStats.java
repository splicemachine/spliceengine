package com.splicemachine.hbase.writer;

import com.splicemachine.stats.*;
import com.splicemachine.stats.util.Folders;

/**
 * @author Scott Fines
 * Date: 2/5/14
 */
public class MergingWriteStats implements WriteStats{
		private final Counter bytesWrittenCounter;
		private final Counter rowsWrittenCounter;
		private final Counter retryCounter;
		private final Counter globalErrorCounter;
		private final Counter partialFailureCounter;
		private final Counter rejectedCounter;

		private final MultiTimeView networkTimeView;
		private final MultiTimeView sleepTimeView;
		private final MultiTimeView totalTimeView;

		public MergingWriteStats(MetricFactory metricFactory) {
				this.bytesWrittenCounter = metricFactory.newCounter();
				this.rowsWrittenCounter = metricFactory.newCounter();
				this.retryCounter = metricFactory.newCounter();
				this.globalErrorCounter = metricFactory.newCounter();
				this.partialFailureCounter = metricFactory.newCounter();
				this.rejectedCounter = metricFactory.newCounter();

				this.networkTimeView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
				this.sleepTimeView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
				this.totalTimeView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
		}

		@Override public long getBytesWritten() { return bytesWrittenCounter.getTotal(); }
		@Override public long getRowsWritten() { return rowsWrittenCounter.getTotal(); }
		@Override public long getTotalRetries() { return retryCounter.getTotal(); }
		@Override public long getGlobalErrors() { return globalErrorCounter.getTotal(); }
		@Override public long getPartialFailureCount() { return partialFailureCounter.getTotal(); }
		@Override public long getRejectedCount() { return rejectedCounter.getTotal(); }
		@Override public TimeView getSleepTime() { return sleepTimeView; }
		@Override public TimeView getNetworkTime() { return networkTimeView; }

		@Override public TimeView getTotalTime() { return totalTimeView; }


		public void merge(WriteStats newStats){
				this.bytesWrittenCounter.add(newStats.getBytesWritten());
				this.rowsWrittenCounter.add(newStats.getRowsWritten());
				this.retryCounter.add(newStats.getTotalRetries());
				this.globalErrorCounter.add(newStats.getGlobalErrors());
				this.partialFailureCounter.add(newStats.getPartialFailureCount());
				this.rejectedCounter.add(newStats.getRejectedCount());
				this.networkTimeView.update(newStats.getNetworkTime());
				this.sleepTimeView.update(newStats.getSleepTime());
				this.totalTimeView.update(newStats.getTotalTime());
		}
}
