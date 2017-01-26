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
