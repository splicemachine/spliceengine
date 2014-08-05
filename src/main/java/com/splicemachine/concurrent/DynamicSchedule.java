package com.splicemachine.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author P Trolard
 *         Date: 27/03/2014
 */
public class DynamicSchedule {


    /**
     * Return Runnable that runs r then schedules it to be run again on the scheduled
     * service ses, after a delay of its running time * multiple. If given, floor
     * gives the minimum delay. (Times in millis.)
     */
    public static Runnable runAndScheduleAsMultiple(final Runnable r,
                                                    final ScheduledExecutorService ses,
                                                    final int multiple,
                                                    final int floor) {
        return new Runnable() {
            @Override
            public void run() {
                long begin = System.currentTimeMillis();
                try {
                    r.run();
                } finally {
                    if (!ses.isShutdown()) {
                        long duration = System.currentTimeMillis() - begin;
                        ses.schedule(runAndScheduleAsMultiple(r, ses, multiple, floor),
                                        Math.max(floor, duration * multiple),
                                        TimeUnit.MILLISECONDS);
                    }
                }
            }
        };
    }

    public static Runnable runAndScheduleAsMultiple(final Runnable r,
                                                    final ScheduledExecutorService ses,
                                                    final int multiple) {
        return runAndScheduleAsMultiple(r, ses, multiple, -1);
    }
}
