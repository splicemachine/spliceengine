package com.splicemachine.concurrent;

import java.util.concurrent.TimeUnit;

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

    @Override
    public void sleep(long time,TimeUnit unit) throws InterruptedException{
        nanosTime+=unit.toNanos(time);
    }

    public void increment(long nanos){
        nanosTime+=nanos;
    }
}
