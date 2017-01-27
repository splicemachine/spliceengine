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

