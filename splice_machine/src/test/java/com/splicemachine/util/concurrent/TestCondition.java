/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
