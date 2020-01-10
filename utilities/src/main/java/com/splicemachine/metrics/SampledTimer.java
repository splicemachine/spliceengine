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
 *         Date: 7/21/14
 */
public class SampledTimer implements LatencyTimer,DistributionTimeView{
    private final SampleTimeMeasure wallTimer;
//    private final SampleTimeMeasure cpuTimer;
//    private final SampleTimeMeasure userTimer;

    private long numEvents = 0l;
    public SampledTimer(int sampleSize,
                        TimeMeasure wallTimer,
                        TimeMeasure cpuTimer,
                        TimeMeasure userTimer) {
        this.wallTimer = new SampleTimeMeasure(wallTimer,sampleSize);
//        this.cpuTimer = new SampleTimeMeasure(cpuTimer,sampleSize);
//        this.userTimer = new SampleTimeMeasure(userTimer,sampleSize);
    }

    @Override public long getWallClockTime() { return wallTimer.getElapsedTime(); }
//    @Override public long getCpuTime() { return cpuTimer.getElapsedTime(); }
//    @Override public long getUserTime() { return userTimer.getElapsedTime(); }

    @Override public long getCpuTime() { return 0l; }
    @Override public long getUserTime() { return 0l; }

    @Override public long getStopWallTimestamp() { return wallTimer.getStopTimestamp(); }
    @Override public long getStartWallTimestamp() { return wallTimer.getStartTimestamp(); }
    @Override public long getNumEvents() { return numEvents; }

    @Override
    public void startTiming() {
        wallTimer.startTime();
//        cpuTimer.startTime();
//        userTimer.startTime();
    }

    @Override
    public void stopTiming() {
//        userTimer.stopTime();
//        cpuTimer.stopTime();
        wallTimer.stopTime();
    }

    @Override
    public void tick(long numEvents) {
        stopTiming();
        this.numEvents+=numEvents;
    }

    @Override public LatencyView wallLatency() { return wallTimer; }
//    @Override public LatencyView cpuLatency() { return cpuTimer; }
//    @Override public LatencyView userLatency() { return userTimer; }
    @Override public LatencyView cpuLatency() { return Metrics.noOpLatencyView(); }
    @Override public LatencyView userLatency() { return Metrics.noOpLatencyView();}
    @Override public TimeView getTime() { return this; }

    public DistributionTimeView getDistribution() { return this; }
}
