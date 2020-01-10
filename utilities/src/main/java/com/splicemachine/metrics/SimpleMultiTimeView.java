/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
