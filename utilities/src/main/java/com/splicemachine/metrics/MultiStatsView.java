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
