/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.util.concurrent;

import com.splicemachine.concurrent.Clock;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * @author Scott Fines
 *         Date: 9/4/15
 */
public abstract class TestCondition implements Condition{
    private final Clock clock;

    private boolean signalled = false;
    public TestCondition(Clock clock){
        this.clock=clock;
    }

    @Override
    public void await() throws InterruptedException{
        while(true){
            if(signalled) break;
            waitInterruptibly();
        }
    }

    @Override
    public void awaitUninterruptibly(){
        while(true){
            if(signalled) break;
            waitUninterruptibly();
        }
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException{
        long s = clock.nanoTime();
        while(nanosTimeout>0){
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            if(signalled) break;

            waitInterruptibly();
            long e = clock.nanoTime();
            nanosTimeout-=(e-s);
        }
        return nanosTimeout;
    }

    @Override
    public boolean await(long time,TimeUnit unit) throws InterruptedException{
        signalled = false;
        long timeRemaining = unit.toNanos(time);
        return awaitNanos(timeRemaining)>0;
    }


    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException{
        //this is probably not a good strategy for test clocks, since they may never increment that high
        long time=deadline.getTime();
        long timeRemaining = time-clock.currentTimeMillis();
        return await(timeRemaining,TimeUnit.MILLISECONDS);
    }

    @Override
    public void signal(){
        this.signalled = true;
    }

    @Override public void signalAll(){ signal(); }

    protected abstract void waitUninterruptibly();

    protected void waitInterruptibly() throws InterruptedException{
       waitUninterruptibly();
    }
}
