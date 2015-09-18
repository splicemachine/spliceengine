package com.splicemachine.derby.ddl;

import com.splicemachine.concurrent.TickingClock;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 9/8/15
 */
class IncrementingClock implements TickingClock{
    private int currentTime;

    public IncrementingClock(int startNanoes){
        this.currentTime=startNanoes;
    }

    @Override
    public long tickMillis(long millis){
        this.currentTime+=TimeUnit.MILLISECONDS.toNanos(millis);
        return currentTime;
    }

    @Override
    public long tickNanos(long nanos){
        this.currentTime+=nanos;
        return currentTime;
    }

    @Override
    public long currentTimeMillis(){
        return TimeUnit.NANOSECONDS.toMillis(currentTime);
    }

    @Override
    public long nanoTime(){
        return currentTime;
    }
}
