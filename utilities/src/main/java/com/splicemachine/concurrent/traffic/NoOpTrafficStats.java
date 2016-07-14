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
