package com.splicemachine.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 6/3/13
 */
public class MonitoredThreadPool implements WriterPoolStatus{
    private final ThreadPoolExecutor writerPool;

    private final AtomicInteger numPendingTasks = new AtomicInteger(0);
    private final AtomicLong numFailedTasks = new AtomicLong(0l);
    private final AtomicLong totalSuccessfulTasks = new AtomicLong(0l);

    private MonitoredThreadPool(ThreadPoolExecutor writerPool){
        this.writerPool = writerPool;
    }

    public static MonitoredThreadPool create(){
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("writerpool-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();

        int maxThreads = SpliceConstants.maxThreads;
        long keepAliveSeconds = SpliceConstants.threadKeepAlive;
        ThreadPoolExecutor writerPool = new ThreadPoolExecutor(maxThreads,
                maxThreads,keepAliveSeconds,
                TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(),factory);
        return new MonitoredThreadPool(writerPool);
    }

    public <V> Future<V> submit(Callable<V> task){
        numPendingTasks.incrementAndGet();
        return this.writerPool.submit(new WatchingCallable<V>(task));
    }

    @Override
    public int getPendingTaskCount() {
        return numPendingTasks.get();
    }

    @Override
    public int getActiveThreadCount() {
        return writerPool.getActiveCount();
    }

    @Override
    public int getCurrentThreadCount() {
        return writerPool.getPoolSize();
    }

    @Override
    public long getTotalSubmittedTasks() {
        return writerPool.getTaskCount();
    }

    @Override
    public long getTotalFailedTasks() {
        return numFailedTasks.get();
    }

    @Override
    public long getTotalSuccessfulTasks() {
        return totalSuccessfulTasks.get();
    }

    @Override
    public long getTotalCompletedTasks() {
        return writerPool.getCompletedTaskCount();
    }

    @Override
    public int getMaxThreadCount() {
        return writerPool.getMaximumPoolSize();
    }

    @Override
    public void setMaxThreadCount(int newMaxThreadCount) {
        writerPool.setCorePoolSize(newMaxThreadCount);
    }

    @Override
    public long getThreadKeepAliveTimeMs() {
        return writerPool.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    @Override
    public void setThreadKeepAliveTimeMs(long timeMs) {
        writerPool.setKeepAliveTime(timeMs,TimeUnit.MILLISECONDS);
    }

    @Override
    public int getLargestThreadCount() {
        return writerPool.getLargestPoolSize();
    }

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

}
