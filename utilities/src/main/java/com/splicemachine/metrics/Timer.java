/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
