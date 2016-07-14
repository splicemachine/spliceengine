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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Concurrent "ticking" clock, which is manually moved forward, but in a thread safe manner.
 *
 * In this implementation, "millis" is equal to 1000 ticks, while "nanos" is a single tick.
 *
 * @author Scott Fines
 *         Date: 8/14/15
 */
public class ConcurrentTicker implements TickingClock{
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

    @Override
    public void sleep(long time,TimeUnit unit) throws InterruptedException{
        long toTick = unit.toNanos(time);
        ticker.addAndGet(toTick);
    }

    @Override
    public long tickMillis(long millis){
       return ticker.addAndGet(millis*1000)/1000;
    }

    @Override
    public long tickNanos(long nanos){
        return ticker.addAndGet(nanos);
    }
}
