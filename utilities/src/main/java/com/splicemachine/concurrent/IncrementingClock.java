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
