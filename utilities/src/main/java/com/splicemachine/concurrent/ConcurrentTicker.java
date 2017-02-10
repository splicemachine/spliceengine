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
