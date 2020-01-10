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
