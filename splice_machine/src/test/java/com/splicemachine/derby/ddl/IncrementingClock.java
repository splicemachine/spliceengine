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

    @Override
    public void sleep(long l,TimeUnit timeUnit) throws InterruptedException{
        long n = timeUnit.toNanos(l);
        currentTime+=n;
    }
}
