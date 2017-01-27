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

/**
 * Represents a Traffic Controller that always allows traffic through immediately.
 * @author Scott Fines
 *         Date: 12/9/14
 */
public class GreenLight implements TrafficControl{
    public static final GreenLight INSTANCE = new GreenLight();

    @Override public void release(int permits) {  }
    @Override public int tryAcquire(int minPermits, int maxPermits) { return maxPermits; }
    @Override public void acquire(int permits) throws InterruptedException { }
    @Override public int getAvailablePermits() { return Integer.MAX_VALUE; }
    @Override public int getMaxPermits() { return Integer.MAX_VALUE; }
    @Override public void setMaxPermits(int newMaxPermits) {  }
}
