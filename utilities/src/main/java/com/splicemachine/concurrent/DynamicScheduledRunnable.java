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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A Runnable that runs targetRunnable then schedules itself to be run again on the provided scheduled executor
 * service after a delay.  The delay is a multiple of the last run duration or minDelayMillis, whichever is larger.
 */
public class DynamicScheduledRunnable implements Runnable {

    private final Runnable targetRunnable;
    private final ScheduledExecutorService executorService;
    private final int multiple;
    private final int minDelayMillis;

    public DynamicScheduledRunnable(Runnable targetRunnable, ScheduledExecutorService executorService, int multiple, int minDelayMillis) {
        this.targetRunnable = targetRunnable;
        this.executorService = executorService;
        this.multiple = multiple;
        this.minDelayMillis = minDelayMillis;
    }

    @Override
    public void run() {
        long begin = System.currentTimeMillis();
        try {
            targetRunnable.run();
        } finally {
            if (!executorService.isShutdown()) {
                long durationMillis = System.currentTimeMillis() - begin;
                long delayMillis = Math.max(minDelayMillis, durationMillis * multiple);
                Runnable command = new DynamicScheduledRunnable(targetRunnable, executorService, multiple, minDelayMillis);
                executorService.schedule(command, delayMillis, TimeUnit.MILLISECONDS);
            }
        }
    }
}
