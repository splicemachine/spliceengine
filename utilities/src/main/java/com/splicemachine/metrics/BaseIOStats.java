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
public class BaseIOStats implements IOStats{
		private final TimeView time;
		private final long bytes;
		private final long rows;

		public BaseIOStats(TimeView time, long bytes, long rows) {
				this.time = time;
				this.bytes = bytes;
				this.rows = rows;
		}

		@Override public TimeView getTime() { return time; }

		@Override public long elementsSeen() { return rows; }

		@Override public long bytesSeen() { return bytes; }
}
