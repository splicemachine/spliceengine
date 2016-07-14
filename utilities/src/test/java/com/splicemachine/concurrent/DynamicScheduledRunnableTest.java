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

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertTrue;

public class DynamicScheduledRunnableTest {

    private ScheduledExecutorService executorService = MoreExecutors.namedSingleThreadScheduledExecutor("test");

    @Test
    public void testMinimumDelayUsed() throws InterruptedException {

        // given

        final CountDownLatch countDownLatch = new CountDownLatch(5);
        Runnable targetRunnable = new Runnable() {
            @Override
            public void run() {
                countDownLatch.countDown();
            }
        };
        DynamicScheduledRunnable r = new DynamicScheduledRunnable(targetRunnable, executorService, 1, 500);

        // when

        long startTime = currentTimeMillis();
        executorService.schedule(r, 0, TimeUnit.SECONDS);

        // then

        assertTrue("expected targetRunnable to be invoked 5 times", countDownLatch.await(10, TimeUnit.SECONDS));
        assertTrue((currentTimeMillis() - startTime) >= 1950);
    }

    @Test
    public void testMinimumDelayNotUsed() throws InterruptedException {

        // given

        final CountDownLatch countDownLatch = new CountDownLatch(5);
        Runnable targetRunnable = new Runnable() {
            @Override
            public void run() {
                uncheckedSleep(20);
                countDownLatch.countDown();
            }
        };
        DynamicScheduledRunnable r = new DynamicScheduledRunnable(targetRunnable, executorService, 10, 1);

        // when

        long startTime = currentTimeMillis();
        executorService.schedule(r, 0, TimeUnit.SECONDS);

        // then

        assertTrue("expected targetRunnable to be invoked 5 times", countDownLatch.await(10, TimeUnit.SECONDS));
        assertTrue((currentTimeMillis() - startTime) >= 750);
    }


    private void uncheckedSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            //
        }
    }


}