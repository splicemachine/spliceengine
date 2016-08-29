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
