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
