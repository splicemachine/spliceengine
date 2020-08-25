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

package com.splicemachine.metrics;

import com.google.common.util.concurrent.AtomicDouble;
import com.splicemachine.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;

/**
 * Concurrent, Exponentially-weighted moving average.
 *
 * @author Scott Fines
 *         Date: 11/14/14
 */
@ThreadSafe
public class ConcurrentEWMA {

    private final AtomicLong count = new AtomicLong(0l);
    private final double alpha;
    private final TimeUnit intervalUnit;
    private final long interval;

    private volatile long decayTime;

    private volatile boolean initialized = false;
    private final AtomicDouble ewma = new AtomicDouble(0d);

    public ConcurrentEWMA(double alpha, long interval,TimeUnit intervalUnit) {
        this.alpha = alpha*intervalUnit.toNanos(1);
        this.intervalUnit = intervalUnit;
        this.decayTime = System.nanoTime();
        this.interval = intervalUnit.toNanos(interval);
    }

    public void update(long count){
        long total = this.count.addAndGet(count);
        double instantRate = (double)total/interval; //the instantaneous rate
        if(!initialized && ewma.compareAndSet(0d,instantRate)) {
                initialized = true;
                return;
        }
        /*
         * update the rate according to the new formula:
         *
         * ewma = oldEwma + alpha*(instantRate-oldEwma)
         */
        synchronized (this){ //TODO -sf- don't synchronize here
            long ts = System.nanoTime();
            long pastTime = decayTime;
            long tDiff = ts-pastTime;
//            if(tDiff<interval) return; //do nothing if we haven't hit the interval yet
            double scale = exp(-tDiff/alpha);
            double curr = ewma.get();
            double newRate = instantRate+scale*(curr-instantRate);
            decayTime = ts;
            ewma.set(newRate);
        }
    }

    public double rate(){
        update(0); //make sure to account for time differences
        return ewma.get()*intervalUnit.toNanos(1l);
    }


    public static void main(String...args) throws Exception{
        double oneMAlpha = 1*60d;
        new ConcurrentEWMA(oneMAlpha,1,TimeUnit.SECONDS);
    }
}
