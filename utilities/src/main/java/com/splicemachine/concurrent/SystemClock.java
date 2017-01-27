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
