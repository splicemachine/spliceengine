/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
