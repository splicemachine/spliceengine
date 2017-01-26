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

package com.splicemachine.concurrent.traffic;

import java.util.concurrent.locks.LockSupport;

/**
 * Uses exponential backoff to wait up to the specified timeout.
 * The strategy is as follows:
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class RetryBackoffWaitStrategy implements WaitStrategy {
    private final int windowSize;

    public RetryBackoffWaitStrategy(int windowSize) {
        int s = 1;
        while(s<windowSize){
            s<<=1;
        }
        this.windowSize = s;
    }

    @Override
    public void wait(int iterationCount, long nanosLeft, long minWaitNanos) {
        //take the highest power of two <= iterationCount, and that's the scale
        int scale = iterationCount/windowSize+1;

        long wait = Math.min(minWaitNanos*scale, nanosLeft);
        //now park for the minimum wait time
        LockSupport.parkNanos(wait);
    }
}
