package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public class IncrementingClock implements Clock{
    private long nanosTime = 0l;

    @Override
    public long currentTimeMillis(){
        return nanosTime/1000000l;
    }

    @Override
    public long nanoTime(){
        return nanosTime;
    }

    public void increment(long nanos){
        nanosTime+=nanos;
    }
}
