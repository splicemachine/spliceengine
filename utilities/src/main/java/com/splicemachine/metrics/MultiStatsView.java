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

/**
 * @author Scott Fines
 *         Date: 1/24/14
 */
public class MultiStatsView implements IOStats {
		private final MultiTimeView timeView;
		private long totalRows;
		private long totalBytes;

		public MultiStatsView(MultiTimeView timeView) {
				this.timeView = timeView;
		}

		@Override public TimeView getTime() { return timeView; }
		@Override public long elementsSeen() { return totalRows; }
		@Override public long bytesSeen() { return totalBytes; }

		public void update(TimeView time, long totalRows,long totalBytes){
				this.timeView.update(time);
				this.totalRows+= totalRows;
				this.totalBytes+=totalBytes;
		}

		public void merge(IOStats other){
				this.timeView.update(other.getTime());
				this.totalBytes+=other.bytesSeen();
				this.totalRows+=other.elementsSeen();
		}
}
