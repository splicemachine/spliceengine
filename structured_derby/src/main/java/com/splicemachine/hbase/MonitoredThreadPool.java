package com.splicemachine.hbase;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 6/3/13
 */
public class MonitoredThreadPool implements ThreadPoolStatus {
    private final ListeningExecutorService listeningService;
    private final ThreadPoolExecutor writerPool;

    private final AtomicInteger numPendingTasks = new AtomicInteger(0);
    private final AtomicLong numFailedTasks = new AtomicLong(0l);
    private final AtomicLong totalSuccessfulTasks = new AtomicLong(0l);
    private final CountingRejectionHandler countingRejectionHandler;

    private MonitoredThreadPool(ThreadPoolExecutor writerPool, CountingRejectionHandler countingRejectionHandler){
        this.writerPool = writerPool;
        this.listeningService = MoreExecutors.listeningDecorator(writerPool);
        this.countingRejectionHandler = countingRejectionHandler;
    }

    public static MonitoredThreadPool create(){
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("writerpool-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();

        int maxThreads = SpliceConstants.maxThreads;
        int coreThreads = SpliceConstants.coreWriteThreads;
        long keepAliveSeconds = SpliceConstants.threadKeepAlive;
        CountingRejectionHandler countingRejectionHandler = new CountingRejectionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        ThreadPoolExecutor writerPool = new ThreadPoolExecutor(coreThreads,
                maxThreads,keepAliveSeconds,
                TimeUnit.SECONDS,new SynchronousQueue<Runnable>(),factory,
                countingRejectionHandler);
        writerPool.allowCoreThreadTimeOut(true);
        return new MonitoredThreadPool(writerPool, countingRejectionHandler);
    }

    public void shutdown(){
        writerPool.shutdown();
    }

    public <V> ListenableFuture<V> submit(Callable<V> task){
        numPendingTasks.incrementAndGet();
        return this.listeningService.submit(new WatchingCallable<V>(task));
    }

    @Override public int getPendingTaskCount() { return numPendingTasks.get(); }
    @Override public int getActiveThreadCount() { return writerPool.getActiveCount(); }
    @Override public int getCurrentThreadCount() { return writerPool.getPoolSize(); }
    @Override public long getTotalSubmittedTasks() { return writerPool.getTaskCount(); }
    @Override public long getTotalFailedTasks() { return numFailedTasks.get(); }
    @Override public long getTotalSuccessfulTasks() { return totalSuccessfulTasks.get(); }
    @Override public long getTotalCompletedTasks() { return writerPool.getCompletedTaskCount(); }
    @Override public int getMaxThreadCount() { return writerPool.getMaximumPoolSize(); }
    @Override public void setMaxThreadCount(int newMaxThreadCount) { writerPool.setMaximumPoolSize(newMaxThreadCount); }
    @Override public long getThreadKeepAliveTimeMs() { return writerPool.getKeepAliveTime(TimeUnit.MILLISECONDS); }
    @Override public void setThreadKeepAliveTimeMs(long timeMs) { writerPool.setKeepAliveTime(timeMs,TimeUnit.MILLISECONDS); }
    @Override public int getLargestThreadCount() { return writerPool.getLargestPoolSize(); }
    @Override public long getTotalRejectedTasks() { return countingRejectionHandler.getTotalRejected(); }

    private class WatchingCallable<V> implements Callable<V>{
        private final Callable<V> delegate;

        private WatchingCallable(Callable<V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public V call() throws Exception {
            numPendingTasks.decrementAndGet();
            try{
                V item= delegate.call();
                totalSuccessfulTasks.incrementAndGet();
                return item;
            }catch(Exception e){
                numFailedTasks.incrementAndGet();
                throw e;
            }
        }
    }

    private static class CountingRejectionHandler implements RejectedExecutionHandler {
        private final RejectedExecutionHandler delegate;
        private final AtomicLong totalRejected = new AtomicLong(0l);
        public CountingRejectionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.delegate = rejectedExecutionHandler;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            totalRejected.incrementAndGet();
            if(delegate!=null)
                delegate.rejectedExecution(r,executor);
        }

        public long getTotalRejected(){
            return totalRejected.get();
        }
    }
}
