package com.splicemachine.stats;

import java.util.Arrays;
import java.util.Random;

/**
 * @author Scott Fines
 * Date: 7/10/14
 */
public class TimeSample implements TimeMeasure{
    private final TimeMeasure timer;
    private final Random random;

    private final int sampleSize;

    private long[] sampleTimes;

    private long numEvents = 0;
    private boolean sorted = false;

    public TimeSample(TimeMeasure delegate,int sampleSize) {
        this.sampleSize = sampleSize;
        this.timer = delegate;
        this.random = new Random(System.currentTimeMillis());
        this.sampleTimes = new long[Math.min(10,sampleSize)];
    }

    @Override public void startTime() { timer.startTime(); }

    @Override
    public long stopTime() {
        long elapsed = timer.stopTime();
        int sampleSpot;
        if(numEvents <sampleSize)
            sampleSpot = (int) numEvents;
        else
            sampleSpot = random.nextInt((int) numEvents +1);

        numEvents++;

        if(sampleSpot<sampleSize){
            addToSample(elapsed,sampleSpot);
        }
        return elapsed;
    }

    private void addToSample(long elapsed,int pos) {
        if(pos>=sampleTimes.length){
            /*
             * This method is only called until we reach sampleSize, so this block
             * will only be called if sampleTimes.length < sampleSize
             */
            sampleTimes = Arrays.copyOf(sampleTimes,Math.min(sampleSize,2*sampleTimes.length));
        }

        sampleTimes[pos] = elapsed;
    }

    @Override public long getElapsedTime() { return timer.getElapsedTime(); }
    @Override public long getStopTimestamp() { return timer.getStopTimestamp(); }
    @Override public long getStartTimestamp() { return timer.getStartTimestamp(); }

    public long estimateMedianTime(){
        //TODO -sf- replace this with a more efficient selection algorithm
        sortTimes();
        return sampleTimes[sampleTimes.length/2];
    }

    public long estimate75p(){
        sortTimes();
        return sampleTimes[75*sampleTimes.length/100];
    }

    public long estimate90p(){
        sortTimes();
        return sampleTimes[9*sampleTimes.length/10];
    }

    public long estimate95p(){
        sortTimes();
        return sampleTimes[95*sampleTimes.length/100];
    }

    public long estimate99p(){
        sortTimes();
        return sampleTimes[99*sampleTimes.length/100];
    }

    public double estimateAverageTime(){
        return timer.getElapsedTime()/numEvents;
    }

    protected void sortTimes() {
        if(!sorted){
            Arrays.sort(sampleTimes);
            sorted=true;
        }
    }
}
