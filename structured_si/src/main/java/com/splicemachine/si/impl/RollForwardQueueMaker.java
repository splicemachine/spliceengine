package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 8/22/13
 */
public class RollForwardQueueMaker {
    private static final RollForwardQueueMaker INSTANCE = new RollForwardQueueMaker();

    private final ScheduledExecutorService scheduledPool;
    private final ExecutorService loadPool;

    private final long maxHeapSize;
    private final int maxEntries;
    private final long timeout;

    private RollForwardQueueMaker(){
        this.maxHeapSize = SpliceConstants.maxRollForwardHeapSize;
        this.maxEntries = SpliceConstants.maxRollForwardEntries;
        this.timeout = SpliceConstants.rollForwardTimeout;


        int maxScheduledThreads = SpliceConstants.maxRollForwardScheduledThreads;

        ThreadFactory scheduledFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("rollForward-timed-pool-%d").build();
        scheduledPool = Executors.newScheduledThreadPool(maxScheduledThreads,scheduledFactory);


        int maxLoadThreads = SpliceConstants.maxRollForwardLoadThreads;
        int coreLoadThreads = SpliceConstants.maxRollForwardCoreThreads;
        int maxConcurrentRollForwards = SpliceConstants.maxConcurrentRollForwards;
        ThreadFactory loadFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("rollForward-load-pool-%d").build();
        //we silently discard roll forwards that exceed our max limit
        loadPool = new ThreadPoolExecutor(coreLoadThreads,maxLoadThreads,
                60l,TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(maxConcurrentRollForwards),
                loadFactory,
                new ThreadPoolExecutor.DiscardPolicy());
    }

    public RollForwardQueue<byte[],ByteBuffer> createConcurrentQueue(Hasher<byte[],ByteBuffer> hasher,
                                                                     RollForwardAction<byte[]> action){
        ConcurrentRollForwardQueue queue =  new ConcurrentRollForwardQueue(hasher,action,maxHeapSize,maxEntries,timeout,scheduledPool,loadPool);
        queue.start();
        return queue;
    }

    public static RollForwardQueueMaker instance() {
        return INSTANCE;
    }
}
