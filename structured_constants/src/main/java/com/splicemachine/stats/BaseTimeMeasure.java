package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 1/17/14
 */
public abstract class BaseTimeMeasure implements TimeMeasure{
		private long totalTime;

		private long  start;

		@Override
		public void startTime() {
			this.start = getTimestamp();
		}

		protected abstract long getTimestamp();

		@Override
		public void stopTime() {
			this.totalTime+=(getTimestamp()-start);
		}

		@Override
		public long getElapsedTime() {
				return totalTime;
		}
}
