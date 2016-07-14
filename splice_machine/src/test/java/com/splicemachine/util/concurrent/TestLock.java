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
