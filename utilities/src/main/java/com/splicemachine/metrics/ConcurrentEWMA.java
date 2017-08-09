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

package com.splicemachine.metrics;

import org.spark_project.guava.util.concurrent.AtomicDouble;

import javax.annotation.concurrent.ThreadSafe;
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
