package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 1/16/14
 */
public interface Timer {

		/**
		 * Begin recording time.
		 */
		void startTiming();

		/**
		 * stop recording time. Equivalent to {@code tick(0)}.
		 */
		void stopTiming();

		/**
		 * Record an event.
		 *
		 * @param numEvents the number of events that occurred in the time between calling {@link #startTiming()}
		 *                  and calling this.
		 */
		void tick(long numEvents);

		/**
		 * @return the number of recorded events.
		 */
		long getNumEvents();

		TimeView getTime();
}
