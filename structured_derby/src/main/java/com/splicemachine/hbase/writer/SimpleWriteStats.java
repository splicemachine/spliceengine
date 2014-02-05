package com.splicemachine.hbase.writer;

import com.splicemachine.stats.TimeView;

/**
 * @author Scott Fines
 * Date: 2/5/14
 */
public class SimpleWriteStats implements WriteStats {
		private final long bytesWritten;
		private final long rowsWritten;
		private final long totalRetries;
		private final long globalErrors;
		private final long partialFailureCount;
		private final long rejectedCount;
		private final TimeView sleepTime;
		private final TimeView networkTime;
		private final TimeView totalTime;

		public SimpleWriteStats(long bytesWritten,
														long rowsWritten,
														long totalRetries,
														long globalErrors,
														long partialFailureCount,
														long rejectedCount,
														TimeView sleepTime,
														TimeView networkTime,
														TimeView totalTime) {
				this.bytesWritten = bytesWritten;
				this.rowsWritten = rowsWritten;
				this.totalRetries = totalRetries;
				this.globalErrors = globalErrors;
				this.partialFailureCount = partialFailureCount;
				this.rejectedCount = rejectedCount;
				this.sleepTime = sleepTime;
				this.networkTime = networkTime;
				this.totalTime = totalTime;
		}

		@Override public long getBytesWritten() { return bytesWritten; }
		@Override public long getRowsWritten() { return rowsWritten; }
		@Override public long getTotalRetries() { return totalRetries; }
		@Override public long getGlobalErrors() { return globalErrors; }
		@Override public long getPartialFailureCount() { return partialFailureCount; }
		@Override public long getRejectedCount() { return rejectedCount; }
		@Override public TimeView getSleepTime() { return sleepTime; }
		@Override public TimeView getNetworkTime() { return networkTime; }
		@Override public TimeView getTotalTime() { return totalTime; }
}
