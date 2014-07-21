package com.splicemachine.stats;

import java.util.Arrays;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 7/21/14
 */
final class SampleTimeMeasure implements TimeMeasure,LatencyView{
    private final TimeMeasure delegate;
    private final long[] samples;
    private final int sampleSize;
    private long iterationCount = 0l;
    private final Random random = new Random(System.nanoTime());

    protected SampleTimeMeasure(TimeMeasure delegate,int sampleSize) {
        int s = 1;
        while(s<sampleSize)
            s<<=1;

        this.samples = new long[s];
        this.sampleSize = s;
        this.delegate = delegate;
    }

    @Override
    public void startTime() {
        iterationCount++;
        delegate.startTime();
    }

    @Override
    public long stopTime() {
        long elapsedTime = delegate.stopTime();
        int spot = (int)iterationCount;
        if(spot>sampleSize)
            spot = random.nextInt(spot+1);

        if(spot<sampleSize)
            samples[spot] = elapsedTime;
        return elapsedTime;
    }

    @Override public long getElapsedTime() { return delegate.getElapsedTime(); }
    @Override public long getStopTimestamp() { return delegate.getStopTimestamp(); }
    @Override public long getStartTimestamp() { return delegate.getStartTimestamp(); }

    @Override public double getOverallLatency() { return ((double)delegate.getElapsedTime())/iterationCount; }

    @Override
    public long getP50Latency() {
        Arrays.sort(samples);
        return samples[sampleSize/2];
    }

    @Override
    public long getP75Latency() {
        Arrays.sort(samples);
        return samples[75*sampleSize/100];
    }

    @Override
    public long getP90Latency() {
        Arrays.sort(samples);
        return samples[90*sampleSize/100];
    }

    @Override
    public long getP95Latency() {
        Arrays.sort(samples);
        return samples[95*sampleSize/100];
    }

    @Override
    public long getP99Latency() {
        Arrays.sort(samples);
        return samples[99*sampleSize/100];
    }
}
