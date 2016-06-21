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
