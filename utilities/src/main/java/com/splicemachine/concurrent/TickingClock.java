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

package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/14/15
 */
public interface TickingClock extends Clock{

    /**
     * Move forward the clock by {@code millis} milliseconds.
     *
     * @param millis the millisecond to move forward by
     * @return the value of the clock (in milliseconds) after ticking forward
     */
    long tickMillis(long millis);

    /**
     * Move forward the clock by {@code nanos} nanoseconds.
     *
     * @param nanos the nanos to move forward by
     * @return the value of the clock (in nanoseconds) after moving forward.
     */
    long tickNanos(long nanos);
}
