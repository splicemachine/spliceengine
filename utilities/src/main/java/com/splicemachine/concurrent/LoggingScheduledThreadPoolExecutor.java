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

import org.apache.log4j.Logger;

import java.util.concurrent.*;

/**
 * A ScheduledThreadPoolExecutor that logs exceptions.
 *
 * Find unchecked exceptions thrown by scheduled tasks faster.
 *
 * ScheduledThreadPoolExecutor has the unexpected (evil?) behavior that it catches and SWALLOWS exceptions thrown by
 * your scheduled runnable/callable and then silently stops running it.  It does not log, it does not rethrow to be handled
 * by an UncaughtExceptionHandler (even if the supplied thread factory has one). What this means in practice is that
 * anyone using an ScheduledThreadPoolExecutor must catch not just Exception in their run() implementation, but
 * catch Throwable, if they want to be aware of programming error type exceptions.  This is difficult to enforce and
 * remember, especially on a large project, and especially since it not the case for normal ThreadPoolExecutors.
 */
class LoggingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = Logger.getLogger(LoggingScheduledThreadPoolExecutor.class);

    LoggingScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    /* Intentionally just overriding four methods exposed via ScheduledExecutorService interface, currently
     * the only way users of this package-private class will view it. */

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return super.schedule(new LoggingRunnable(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return super.schedule(new LoggingCallable<V>(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(new LoggingRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(new LoggingRunnable(command), initialDelay, delay, unit);
    }

    private static class LoggingRunnable implements Runnable {
        private Runnable theRunnable;

        LoggingRunnable(Runnable theRunnable) {
            this.theRunnable = theRunnable;
        }

        @Override
        public void run() {
            try {
                theRunnable.run();
            } catch (Throwable e) {
                LOG.error("uncaught exception in ScheduledThreadPoolExecutor", e);
                // throw so that ScheduledThreadPoolExecutor will do what it normally would (not much)
                throw new RuntimeException(e);
            }
        }
    }

    private static class LoggingCallable<T> implements Callable<T> {
        private Callable<T> theRunnable;

        LoggingCallable(Callable<T> theRunnable) {
            this.theRunnable = theRunnable;
        }

        @Override
        public T call() {
            try {
                return theRunnable.call();
            } catch (Throwable e) {
                LOG.error("uncaught exception in ScheduledThreadPoolExecutor", e);
                // throw so that ScheduledThreadPoolExecutor will do what it normally would (not much)
                throw new RuntimeException(e);
            }
        }
    }

}