/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.utils;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;

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
