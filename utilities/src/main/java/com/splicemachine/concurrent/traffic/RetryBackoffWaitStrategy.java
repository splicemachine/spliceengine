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

package com.splicemachine.concurrent.traffic;

import java.util.concurrent.locks.LockSupport;

/**
 * Uses exponential backoff to wait up to the specified timeout.
 * The strategy is as follows:
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class RetryBackoffWaitStrategy implements WaitStrategy {
    private final int windowSize;

    public RetryBackoffWaitStrategy(int windowSize) {
        int s = 1;
        while(s<windowSize){
            s<<=1;
        }
        this.windowSize = s;
    }

    @Override
    public void wait(int iterationCount, long nanosLeft, long minWaitNanos) {
        //take the highest power of two <= iterationCount, and that's the scale
        int scale = iterationCount/windowSize+1;

        long wait = Math.min(minWaitNanos*scale, nanosLeft);
        //now park for the minimum wait time
        LockSupport.parkNanos(wait);
    }
}
