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
