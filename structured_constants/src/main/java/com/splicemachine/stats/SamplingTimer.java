package com.splicemachine.stats;

/**
 * Timer which records total events precisely (e.g. totalElapsedTime is an accurate measure of total time),
 * but which also maintains a sample of individual events.
 *
 * @author Scott Fines
 * Date: 7/10/14
 */
public class SamplingTimer implements Timer,DistributionTimeView{
    private final TimeSample wallTimer;
    private final TimeSample cpuTimer;
    private final TimeSample userTimer;

    private long numEvents;

    public SamplingTimer(int sampleSize) {
        wallTimer = new TimeSample(Metrics.wallTimeMeasure(),sampleSize);
        cpuTimer = new TimeSample(Metrics.cpuTimeMeasure(),sampleSize);
        userTimer = new TimeSample(Metrics.userTimeMeasure(),sampleSize);
    }

    @Override public long getWallClockTime() { return wallTimer.getElapsedTime(); }
    @Override public long getCpuTime() { return cpuTimer.getElapsedTime(); }
    @Override public long getUserTime() { return userTimer.getElapsedTime(); }
    @Override public long getStopWallTimestamp() { return wallTimer.getStopTimestamp(); }
    @Override public long getStartWallTimestamp() { return wallTimer.getStartTimestamp(); }
    @Override public long getNumEvents() { return numEvents; }
    @Override public TimeView getTime() { return this; }

    @Override
    public void startTiming() {
        wallTimer.startTime();
        cpuTimer.startTime();
        userTimer.startTime();
    }

    @Override
    public void stopTiming() {
        wallTimer.stopTime();
        cpuTimer.stopTime();
        userTimer.stopTime();
    }

    @Override
    public void tick(long numEvents) {
        stopTiming();
        this.numEvents+=numEvents;
    }

    @Override public double getAverageWallTime() { return wallTimer.estimateAverageTime(); }
    @Override public long getMedianWallTime() { return wallTimer.estimateMedianTime(); }
    @Override public long getWallTime75p() { return wallTimer.estimate75p(); }
    @Override public long getWallTime90p() { return wallTimer.estimate90p(); }
    @Override public long getWallTime95p() { return wallTimer.estimate95p(); }
    @Override public long getWallTime99p() { return wallTimer.estimate99p(); }

    @Override public double getAverageCpuTime() { return cpuTimer.estimateAverageTime(); }
    @Override public long getMedianCpuTime() { return cpuTimer.estimateMedianTime(); }
    @Override public long getCpuTime75p() { return cpuTimer.estimate75p(); }
    @Override public long getCpuTime90p() { return cpuTimer.estimate90p(); }
    @Override public long getCpuTime95p() { return cpuTimer.estimate95p(); }
    @Override public long getCpuTime99p() { return cpuTimer.estimate99p(); }

    @Override public double getAverageUserTime() { return userTimer.estimateAverageTime(); }
    @Override public long getMedianUserTime() { return userTimer.estimateMedianTime(); }
    @Override public long getUserTime75p() { return userTimer.estimate75p(); }
    @Override public long getUserTime90p() { return userTimer.estimate90p(); }
    @Override public long getUserTime95p() { return userTimer.estimate95p(); }
    @Override public long getUserTime99p() { return userTimer.estimate99p(); }
}
