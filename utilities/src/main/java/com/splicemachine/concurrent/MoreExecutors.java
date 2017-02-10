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

import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Convenient executor factory methods.
 *
 * On daemon property: For the splicemachine main product where we run as part of of HBase we probably want
 * most thread pools to consist of daemons that do not not prevent HBase region server JVMs from shutting down.
 * An thread pool that strictly must finish any in progress work before the jvm exists would be an exception to this
 * rule.  In this later case make the threads non-daemon and make sure ExecutorService.shutdown() is explicitly
 * invoked as part of the splice shutdown process.
 */
public class MoreExecutors {

    private MoreExecutors() {
    }

    /**
     * Single thread ExecutorService with named threads.
     *
     * @param nameFormat name the thread. It's helpful to find in thread dumps, profiler, etc,
     *                   if we name our threads "splice-[something]".
     * @param isDaemon set the daemon property of the thread. <b>NOTE</b>: if setting your thread
     *                 to non-daemon (isDaemon arg is false), you're taking over the responsibility
     *                 of cleaning up any thread resources and shutting down your thread pool.
     */
    public static ExecutorService namedSingleThreadExecutor(String nameFormat, boolean isDaemon) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(isDaemon)
                .build();
        return Executors.newSingleThreadExecutor(factory);
    }

    /**
     * Single thread ScheduledExecutorService with named threads. Thread daemon property is set
     * to true.
     * <p/>
     * Returns an instance that will log any exception not caught by the scheduled task.  Like
     * ScheduledThreadPoolExecutor, returned instance stops scheduled task on exception.
     *
     * @param nameFormat name the thread. It's helpful to find in thread dumps, profiler, etc,
     *                   if we name our threads "splice-[something]".
     */
    public static ScheduledExecutorService namedSingleThreadScheduledExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .build();
        return new LoggingScheduledThreadPoolExecutor(1, factory);
    }

    /**
     * Factory for general purpose ThreadPoolExecutor.
     *
     * @param coreWorkers argument passed to {@link ThreadPoolExecutor}
     * @param maxWorkers  argument passed to {@link ThreadPoolExecutor}
     * @param nameFormat name the thread. It's helpful to find in thread dumps, profiler, etc,
     *                   if we name our threads "splice-[something]".
     * @param keepAliveSeconds argument passed to {@link ThreadPoolExecutor}
     * @param isDaemon set the daemon property of the thread. <b>NOTE</b>: if setting your thread
     *                 to non-daemon (isDaemon arg is false), you're taking over the responsibility
     *                 of cleaning up any thread resources and shutting down your thread pool.
     */
    public static ThreadPoolExecutor namedThreadPool(int coreWorkers, int maxWorkers,
                                                     String nameFormat,
                                                     long keepAliveSeconds,
                                                     boolean isDaemon) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(isDaemon)
                .build();
        return new ThreadPoolExecutor(coreWorkers, maxWorkers, keepAliveSeconds,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), factory);
    }

}
