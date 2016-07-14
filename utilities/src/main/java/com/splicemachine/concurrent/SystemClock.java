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
