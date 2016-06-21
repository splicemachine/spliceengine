package com.splicemachine.concurrent;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class ThreadsTest {

    @Test
    public void sleep() {
        long startTime = System.currentTimeMillis();

        Threads.sleep(100, TimeUnit.MILLISECONDS);

        long elapsedTime = System.currentTimeMillis() - startTime;
        assertTrue(elapsedTime < 1000);
        assertTrue(elapsedTime > 75);
    }

}