package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public abstract class BaseTimer implements Timer {
		private long startTime;
		private long totalTime;
		private long numEvents;


		@Override
		public void startTiming() {
				startTime = getTime();
		}

		protected abstract long getTime();

		@Override
		public void stopTiming() {
				tick(0);
		}

		@Override
		public void tick(long numEvents) {
				this.numEvents+=numEvents;
				long stopTime = getTime();
				totalTime+=(stopTime- startTime);
		}

		@Override public long getNumEvents() { return numEvents; }

		@Override public long getWallClockTime() { return 0; }
		@Override public long getCpuTime() { return 0; }
		@Override public long getUserTime() { return 0; }

		protected long getElapsedTime(){ return totalTime; }
}
