package com.splicemachine.utils;

import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;

/**
 * An unfortunate abstraction that will allow us better control
 * over when a particular thread or entity is put to sleep (allows
 * us to remove sleeps for testing and stuff like that).
 *
 * @author Scott Fines
 * Date: 1/31/14
 */
public interface Sleeper {

		void sleep(long wait) throws InterruptedException;

		TimeView getSleepStats();

		public static Sleeper THREAD_SLEEPER = new Sleeper() {
				@Override public void sleep(long wait) throws InterruptedException { Thread.sleep(wait); }
				@Override public TimeView getSleepStats() { return Metrics.noOpTimeView(); }
		};

		public static class TimedSleeper implements Sleeper{
				private final Timer sleepTimer;
				private final Sleeper delegate;

				public TimedSleeper(Sleeper delegate,MetricFactory metricFactory) {
						this.delegate = delegate;
						this.sleepTimer = metricFactory.newTimer();
				}

				@Override
				public void sleep(long wait) throws InterruptedException {
						sleepTimer.startTiming();
						delegate.sleep(wait);
						sleepTimer.tick(1);
				}

				@Override public TimeView getSleepStats() { return sleepTimer.getTime(); }
		}
}
