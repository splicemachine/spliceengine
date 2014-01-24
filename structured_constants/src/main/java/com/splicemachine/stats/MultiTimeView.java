package com.splicemachine.stats;

import com.splicemachine.stats.util.LongLongFolder;

/**
 * @author Scott Fines
 * Date: 1/24/14
 */
public class MultiTimeView implements TimeView {
		private long totalWallTime;
		private long totalCpuTime;
		private long totalUserTime;
		private long startTimestamp;
		private long stopTimestamp;

		private final LongLongFolder wallTimeFolder;
		private final LongLongFolder cpuFolder;
		private final LongLongFolder userFolder;
		private final LongLongFolder startTimestampFolder;
		private final LongLongFolder stopTimestampFolder;

		public MultiTimeView(LongLongFolder wallTimeFolder,
												 LongLongFolder cpuFolder,
												 LongLongFolder userFolder,
												 LongLongFolder startTimestampFolder,
												 LongLongFolder stopTimestampFolder){
				this.wallTimeFolder = wallTimeFolder;
				this.cpuFolder = cpuFolder;
				this.userFolder = userFolder;
				this.startTimestampFolder = startTimestampFolder;
				this.stopTimestampFolder = stopTimestampFolder;
		}

		@Override public long getWallClockTime() { return totalWallTime; }
		@Override public long getCpuTime() { return totalCpuTime; }
		@Override public long getUserTime() { return totalUserTime; }
		@Override public long getStopWallTimestamp() { return stopTimestamp; }
		@Override public long getStartWallTimestamp() { return startTimestamp; }

		public void update(TimeView timeView){
				totalWallTime = wallTimeFolder.fold(totalWallTime,timeView.getWallClockTime());
				totalCpuTime = cpuFolder.fold(totalCpuTime,timeView.getCpuTime());
				totalUserTime = userFolder.fold(totalUserTime,timeView.getUserTime());
				startTimestamp = startTimestampFolder.fold(startTimestamp,timeView.getStartWallTimestamp());
				stopTimestamp = stopTimestampFolder.fold(stopTimestamp,timeView.getStopWallTimestamp());
		}
}
