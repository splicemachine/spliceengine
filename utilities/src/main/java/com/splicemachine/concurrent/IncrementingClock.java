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

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public class IncrementingClock implements Clock{
    private long nanosTime = 0l;

    @Override
    public long currentTimeMillis(){
        return nanosTime/1000000l;
    }

    @Override
    public long nanoTime(){
        return nanosTime;
    }

    @Override
    public void sleep(long time,TimeUnit unit) throws InterruptedException{
        nanosTime+=unit.toNanos(time);
    }

    public void increment(long nanos){
        nanosTime+=nanos;
    }
}
