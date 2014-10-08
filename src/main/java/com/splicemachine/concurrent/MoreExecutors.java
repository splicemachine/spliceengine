package com.splicemachine.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Convenient executor factory methods.
 *
 * On daemon property: For the splicemachine main product where we run as part of of HBase we probably want
 * all threads to be daemons so as to not prevent HBase from shutting down.  However we already have some non-daemon
 * threads and it doesn't seem to be affecting HBase shutdown-- perhaps region servers eventually call System.exit(),
 * in which case daemon property of out application threads just doesn't matter.  In any case leaving existing values
 * of daemon unchanged in these factory methods for now.
 */
public class MoreExecutors {

    private MoreExecutors() {
    }

    /**
     * Single thread ExecutorService with named threads.
     */
    public static ExecutorService namedSingleThreadExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(false)
                .build();
        return Executors.newSingleThreadExecutor(factory);
    }

    /**
     * Single thread ScheduledExecutorService with named threads.
     *
     * Returns an instance that will log any exception not caught by the scheduled task.  Like
     * ScheduledThreadPoolExecutor, returned instance stops scheduled task on exception.
     */
    public static ScheduledExecutorService namedSingleThreadScheduledExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(false)
                .build();
        return new LoggingScheduledThreadPoolExecutor(1, factory);
    }

    /**
     * Factory for general purpose ThreadPoolExecutor.
     */
    public static ThreadPoolExecutor namedThreadPool(int coreWorkers, int maxWorkers,
                                                     String nameFormat,
                                                     long keepAliveSeconds,
                                                     boolean daemon) {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(daemon)
                .build();
        return new ThreadPoolExecutor(coreWorkers, maxWorkers, keepAliveSeconds,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), factory);
    }

}
