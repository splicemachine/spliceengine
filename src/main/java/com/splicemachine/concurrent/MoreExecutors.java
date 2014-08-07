package com.splicemachine.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 8/5/14
 */
public class MoreExecutors {

    private MoreExecutors(){}

    public static ExecutorService namedSingleThreadExecutor(String nameFormat) {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
        return Executors.newSingleThreadExecutor(factory);
    }

    public static ThreadPoolExecutor namedThreadPool(int coreWorkers,int maxWorkers,
                                                     String nameFormat,
                                                     long keepAliveSeconds,
                                                     boolean daemon){
        ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(daemon).setNameFormat(nameFormat).build();
        return new ThreadPoolExecutor(coreWorkers,maxWorkers,keepAliveSeconds,
                TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(),factory);
    }

}
