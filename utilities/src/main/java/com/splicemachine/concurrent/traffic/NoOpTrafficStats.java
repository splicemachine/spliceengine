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

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public class NoOpTrafficStats implements TrafficStats{
    static final TrafficStats INSTANCE = new NoOpTrafficStats();

    @Override public long totalPermitsRequested() { return 0; }
    @Override public long totalPermitsGranted() { return 0; }
    @Override public double permitThroughput() { return 0; }
    @Override public double permitThroughput1M() { return 0; }
    @Override public double permitThroughput5M() { return 0; }
    @Override public double permitThroughput15M() { return 0; }
    @Override public long totalRequests() { return 0; }
    @Override public long avgRequestLatency() { return 0; }
}
