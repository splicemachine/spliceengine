/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MoreExecutorsTest {

    @Test
    public void newSingleThreadExecutor_usesThreadWithExpectedName() throws Exception {
        ExecutorService executorService = MoreExecutors.namedSingleThreadExecutor("testName-%d", false);
        Future<String> threadName = executorService.submit(new GetThreadNameCallable());
        Future<Boolean> isDaemon = executorService.submit(new IsDaemonCallable());

        assertTrue(threadName.get().matches("testName-\\d"));
        assertEquals(false, isDaemon.get());
        executorService.shutdown();
    }

    @Test
    public void namedSingleThreadScheduledExecutor() throws Exception {
        ScheduledExecutorService executorService = MoreExecutors.namedSingleThreadScheduledExecutor("testName-%d");
        Future<String> threadName = executorService.submit(new GetThreadNameCallable());
        Future<Boolean> isDaemon = executorService.submit(new IsDaemonCallable());

        assertTrue(threadName.get().matches("testName-\\d"));
        assertEquals(true, isDaemon.get());
        executorService.shutdown();
    }

    @Test
    public void namedThreadPool() throws Exception {
        final int CORE_POOL_SIZE = 1 + new Random().nextInt(9);
        final int MAX_POOL_SIZE = CORE_POOL_SIZE + new Random().nextInt(9);
        final int KEEP_ALIVE_SEC = new Random().nextInt(100);
        final boolean IS_DAEMON = new Random().nextBoolean();
        ThreadPoolExecutor executorService = MoreExecutors.namedThreadPool(CORE_POOL_SIZE, MAX_POOL_SIZE, "testName-%d", KEEP_ALIVE_SEC, IS_DAEMON);
        Future<String> threadName = executorService.submit(new GetThreadNameCallable());
        Future<Boolean> isDaemon = executorService.submit(new IsDaemonCallable());

        assertEquals(CORE_POOL_SIZE, executorService.getCorePoolSize());
        assertEquals(MAX_POOL_SIZE, executorService.getMaximumPoolSize());
        assertEquals(KEEP_ALIVE_SEC, executorService.getKeepAliveTime(TimeUnit.SECONDS));
        assertEquals(IS_DAEMON, isDaemon.get());
        assertTrue(threadName.get().matches("testName-\\d"));
        executorService.shutdown();
    }

    private static class GetThreadNameCallable implements Callable<String> {
        @Override
        public String call() throws Exception {
            return Thread.currentThread().getName();
        }
    }

    private static class IsDaemonCallable implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            return Thread.currentThread().isDaemon();
        }
    }
}