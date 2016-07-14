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

package com.splicemachine.metrics;

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

		protected long numEvents;

		public CompositeTimer(TimeMeasure wallClockTime, TimeMeasure userTime, TimeMeasure cpuTime) {
				this.wallClockTime = wallClockTime;
				this.userTime = userTime;
				this.cpuTime = cpuTime;
		}

		@Override
		public void startTiming() {
				cpuTime.startTime();
				userTime.startTime();
				wallClockTime.startTime();
		}

		@Override
		public void stopTiming() {
				wallClockTime.stopTime();
				userTime.stopTime();
				cpuTime.stopTime();
		}

		@Override
		public void tick(long numEvents) {
				stopTiming();
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

