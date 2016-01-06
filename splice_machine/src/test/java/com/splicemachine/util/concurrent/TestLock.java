package com.splicemachine.util.concurrent;

import com.splicemachine.concurrent.Clock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * a "Lock" which can be used for testing in a single-threaded environment. All methods
 *
 * @author Scott Fines
 *         Date: 9/4/15
 */
public abstract class TestLock implements Lock{
    private final Clock clock;

    public TestLock(Clock clock){
        this.clock=clock;
    }

    @Override
    public void lock(){
        while(true){
            if(tryLock()) return;
            blockUninterruptibly();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException{
        while(true){
            if(interrupted())
                throw new InterruptedException();
            if(tryLock()) return;
            blockInterruptibly();
        }
    }

    @Override
    public boolean tryLock(long time,TimeUnit unit) throws InterruptedException{
        long timeRemainingNanos = unit.toNanos(time);
        long s = clock.nanoTime();
        while(timeRemainingNanos>0){
            if(interrupted())
                throw new InterruptedException();
            if(tryLock()) return true;
            blockInterruptibly();
            long e=clock.nanoTime();
            timeRemainingNanos-=(e-s);
            s = e;
        }
        return false;
    }


    @Override public void unlock(){ }

    protected boolean interrupted(){
        return Thread.currentThread().isInterrupted();
    }


    protected void blockInterruptibly() throws InterruptedException{
       blockUninterruptibly();
    }

    protected abstract void blockUninterruptibly();

}
