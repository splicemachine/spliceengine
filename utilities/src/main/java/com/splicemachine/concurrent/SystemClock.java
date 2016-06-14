package com.splicemachine.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public class SystemClock implements Clock{
    public static final Clock INSTANCE = new SystemClock();
    @Override public long currentTimeMillis(){ return System.currentTimeMillis(); }
    @Override public long nanoTime(){ return System.nanoTime(); }

    @Override
    public void sleep(long time,TimeUnit unit) throws InterruptedException{
        long nanosRemaining = unit.toNanos(time);
        while(nanosRemaining>0){
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            long s = System.nanoTime();
            LockSupport.parkNanos(nanosRemaining);
            nanosRemaining -=(System.nanoTime()-s);
        }
    }
}
