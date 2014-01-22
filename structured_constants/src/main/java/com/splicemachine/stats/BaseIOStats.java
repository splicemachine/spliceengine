package com.splicemachine.stats;

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

		@Override public long getRows() { return rows; }

		@Override public long getBytes() { return bytes; }
}
