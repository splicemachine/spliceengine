package com.splicemachine.stats;

/**
 * A Timer instance that monitors User, Cpu, and Wall-clock time.
 *
 * This is a convenience tool so that we don't have to keep 3 timers
 * all over the place.
 *
 * @author Scott Fines
 * Date: 1/17/14
 */
class CompositeTimer implements Timer,TimeView{
		private final TimeMeasure wallClockTime;
		private final TimeMeasure userTime;
		private final TimeMeasure cpuTime;

		private long numEvents;

		public CompositeTimer(TimeMeasure wallClockTime, TimeMeasure userTime, TimeMeasure cpuTime) {
				this.wallClockTime = wallClockTime;
				this.userTime = userTime;
				this.cpuTime = cpuTime;
		}

		@Override
		public void startTiming() {
				wallClockTime.startTime();
				cpuTime.startTime();
				userTime.startTime();
		}

		@Override
		public void stopTiming() {
			tick(0);
		}

		@Override
		public void tick(long numEvents) {
				userTime.stopTime();
				cpuTime.stopTime();
				wallClockTime.stopTime();
				this.numEvents+=numEvents;
		}

		@Override public TimeView getTime() { return this; }
		@Override public long getNumEvents() { return numEvents; }
		@Override public long getWallClockTime() { return wallClockTime.getElapsedTime(); }
		@Override public long getCpuTime() { return cpuTime.getElapsedTime(); }
		@Override public long getUserTime() { return userTime.getElapsedTime(); }
		@Override public long getStopWallTimestamp() { return wallClockTime.getStopTimestamp(); }
		@Override public long getStartWallTimestamp() { return wallClockTime.getStartTimestamp(); }
}

