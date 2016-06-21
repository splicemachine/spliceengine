package com.splicemachine.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for working with CountDownLatch.
 */
public class CountDownLatches {

    /**
     * If calling await in a context where the calling thread being interrupted is unexpected/programmer-error then
     * this unchecked version is more concise.
     */
    public static void uncheckedAwait(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * If calling await in a context where the calling thread being interrupted is unexpected/programmer-error then
     * this unchecked version is more concise.
     */
    public static boolean uncheckedAwait(CountDownLatch countDownLatch, long timeout, TimeUnit timeUnit) {
        try {
            return countDownLatch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


}
