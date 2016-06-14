package com.splicemachine.metrics;

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
    private long minElapsedTime = Long.MAX_VALUE;
    private long maxElapsedTime = 0l;

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
        delegate.startTime();
    }

    @Override
    public long stopTime() {
        long elapsedTime = delegate.stopTime();
        if(elapsedTime<minElapsedTime)
            minElapsedTime = elapsedTime;
        if(elapsedTime > maxElapsedTime)
            maxElapsedTime = elapsedTime;
        int spot = (int)iterationCount;
        if(spot>sampleSize)
            spot = random.nextInt(spot+1);

        if(spot<sampleSize)
            samples[spot] = elapsedTime;
        iterationCount++;
        return elapsedTime;
    }

    @Override public long getElapsedTime() { return delegate.getElapsedTime(); }
    @Override public long getStopTimestamp() { return delegate.getStopTimestamp(); }
    @Override public long getStartTimestamp() { return delegate.getStartTimestamp(); }

    @Override public double getOverallLatency() { return ((double)delegate.getElapsedTime())/iterationCount; }

    @Override
    public long getP25Latency() {
        return get(0.25f);
    }

    private long get(float quartile) {
        Arrays.sort(samples,0,Math.min(samples.length,(int)iterationCount));
        int pos = (int)(quartile*Math.min(sampleSize,iterationCount));
        return samples[pos];
    }

    @Override
    public long getP50Latency() {
        return get(0.5f);
    }

    @Override
    public long getP75Latency() {
        return get(0.75f);
    }

    @Override
    public long getP90Latency() {
        return get(0.90f);
    }

    @Override
    public long getP95Latency() {
        return get(0.95f);
    }

    @Override
    public long getP99Latency() {
        return get(0.99f);
    }

    @Override public long getMinLatency() { return iterationCount == 0 ? 0 : minElapsedTime; }
    @Override public long getMaxLatency() { return iterationCount == 0 ? 0: maxElapsedTime; }
}
