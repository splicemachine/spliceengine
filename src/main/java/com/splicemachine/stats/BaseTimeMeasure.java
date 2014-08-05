package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
abstract class BaseTimeMeasure implements TimeMeasure{
		private long totalTime;

		protected long start;
		protected long stop;

		@Override
		public void startTime() {
			this.start = getTimestamp();
		}

		protected abstract long getTimestamp();

		@Override
		public long stopTime() {
				this.stop = getTimestamp();
				long elapsedTime = stop - start;
				this.totalTime+= elapsedTime;
				return elapsedTime;
		}

		@Override public long getElapsedTime() { return totalTime; }
		@Override public long getStopTimestamp() { return stop; }
		@Override public long getStartTimestamp() { return start; }
}
