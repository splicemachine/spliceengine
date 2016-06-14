package com.splicemachine.concurrent;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountDownLatchesTest {

    @Test
    public void testUncheckedAwait() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                latch.countDown();
            }
        }.start();

        CountDownLatches.uncheckedAwait(latch);
    }

    @Test
    public void testUncheckedAwaitWithTimeout() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                latch.countDown();
            }
        }.start();

        assertTrue(CountDownLatches.uncheckedAwait(latch, 5, TimeUnit.SECONDS));
    }

    @Test
    public void testUncheckedAwaitWithTimeoutReturnsFalse() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                Threads.sleep(5, TimeUnit.SECONDS);
            }
        }.start();

        assertFalse(CountDownLatches.uncheckedAwait(latch, 50, TimeUnit.MILLISECONDS));
    }

}