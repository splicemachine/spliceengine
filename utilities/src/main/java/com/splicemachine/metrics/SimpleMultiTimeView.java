/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.metrics;

import com.splicemachine.metrics.util.LongLongFolder;

/**
 * @author Scott Fines
 * Date: 1/24/14
 */
public class SimpleMultiTimeView implements MultiTimeView {
		private long totalWallTime;
		private long totalCpuTime;
		private long totalUserTime;
		private long startTimestamp = Long.MAX_VALUE;
		private long stopTimestamp;

		private final LongLongFolder wallTimeFolder;
		private final LongLongFolder cpuFolder;
		private final LongLongFolder userFolder;
		private final LongLongFolder startTimestampFolder;
		private final LongLongFolder stopTimestampFolder;

		public SimpleMultiTimeView(LongLongFolder wallTimeFolder,
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
		@Override public long getStartWallTimestamp() {
				if(startTimestamp==Long.MAX_VALUE) return 0l;
				return startTimestamp;
		}

		@Override
		public void update(TimeView timeView){
				totalWallTime = wallTimeFolder.fold(totalWallTime,timeView.getWallClockTime());
				totalCpuTime = cpuFolder.fold(totalCpuTime,timeView.getCpuTime());
				totalUserTime = userFolder.fold(totalUserTime,timeView.getUserTime());
				startTimestamp = startTimestampFolder.fold(startTimestamp,timeView.getStartWallTimestamp());
				stopTimestamp = stopTimestampFolder.fold(stopTimestamp,timeView.getStopWallTimestamp());
		}
}
