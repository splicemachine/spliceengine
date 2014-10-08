package com.splicemachine.concurrent;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MoreExecutorsTest {

    @Test
    public void newSingleThreadExecutor_usesThreadWithExpectedName() throws Exception {
        ExecutorService executorService = MoreExecutors.namedSingleThreadExecutor("testName-%d");
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