package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 5/13/14
 */
public abstract class SimpleTimer implements Timer,TimeView {
		protected final TimeMeasure timeMeasure;

		private long numEvents = 0l;

		public SimpleTimer(TimeMeasure timeMeasure) {
				this.timeMeasure = timeMeasure;
		}

		@Override public void startTiming() { timeMeasure.startTime(); }

		@Override public void stopTiming() { timeMeasure.stopTime(); }

		@Override
		public void tick(long numEvents) {
			timeMeasure.stopTime();
				this.numEvents+=numEvents;
		}

		@Override
		public long getNumEvents() {
				return numEvents;
		}

		@Override
		public TimeView getTime() {
				return this;
		}
}
