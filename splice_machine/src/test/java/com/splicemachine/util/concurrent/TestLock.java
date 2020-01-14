/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
