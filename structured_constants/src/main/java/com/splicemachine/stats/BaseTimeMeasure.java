package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public abstract class BaseTimeMeasure implements TimeMeasure{
		private long totalTime;

		protected long start;
		protected long stop;

		@Override
		public void startTime() {
			this.start = getTimestamp();
		}

		protected abstract long getTimestamp();

		@Override
		public void stopTime() {
				this.stop = getTimestamp();
				this.totalTime+=(stop-start);
		}

		@Override public long getElapsedTime() { return totalTime; }
		@Override public long getStopTimestamp() { return stop; }
		@Override public long getStartTimestamp() { return start; }
}
