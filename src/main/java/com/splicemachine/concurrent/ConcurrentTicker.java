package com.splicemachine.concurrent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Concurrent "ticking" clock, which is manually moved forward, but in a thread safe manner.
 *
 * In this implementation, "millis" is equal to 1000 ticks, while "nanos" is a single tick.
 *
 * @author Scott Fines
 *         Date: 8/14/15
 */
public class ConcurrentTicker implements Clock{
    private final AtomicLong ticker;

    public ConcurrentTicker(long seed){
        this.ticker = new AtomicLong(seed);
    }

    @Override
    public long currentTimeMillis(){
        return ticker.get()/1000;
    }

    @Override
    public long nanoTime(){
        return ticker.get();
    }

    public void tickMillis(long millis){
       ticker.addAndGet(millis*1000);
    }

    public void tickNanos(long nanos){
        ticker.addAndGet(nanos);
    }
}
