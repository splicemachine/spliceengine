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

import com.splicemachine.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe, updateable version of TrafficStats. Useful
 * for keeping running statistics counts.
 *
 * @author Scott Fines
 *         Date: 11/14/14
 */
@ThreadSafe
public class MutableTrafficStats implements TrafficStats{
    AtomicLong totalRequests = new AtomicLong(0l);
    AtomicLong totalPermitsRequested = new AtomicLong(0l);
    AtomicLong totalPermitsGranted = new AtomicLong(0l);

    @Override public long totalPermitsRequested() { return totalPermitsRequested.get(); }
    @Override public long totalPermitsGranted() { return totalPermitsGranted.get(); }

    @Override
    public double permitThroughput() {
        return 0;
    }

    @Override
    public double permitThroughput1M() {
        return 0;
    }

    @Override
    public double permitThroughput5M() {
        return 0;
    }

    @Override
    public double permitThroughput15M() {
        return 0;
    }

    @Override
    public long totalRequests() {
        return 0;
    }

    @Override
    public long avgRequestLatency() {
        return 0;
    }
}
