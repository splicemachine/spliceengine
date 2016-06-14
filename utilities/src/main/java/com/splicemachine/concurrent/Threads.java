package com.splicemachine.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * Thread utilities.
 */
public class Threads {

    private Threads() {
    }

    /**
     * Unchecked version of sleep.
     *
     * Use when calling thread being interrupted is unexpected-- thus unchecked exception is appropriate.
     */
    public static void sleep(long duration, TimeUnit timeUnit) {
        try {
            Thread.sleep(timeUnit.toMillis(duration));
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

}
