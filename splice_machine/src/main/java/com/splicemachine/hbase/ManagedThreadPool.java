/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 3/19/13
 */
public class ManagedThreadPool implements ExecutorService,JMXThreadPool {
    private final ThreadPoolExecutor pool;
    private final AtomicInteger totalRejected = new AtomicInteger(0);

    public ManagedThreadPool(ThreadPoolExecutor pool) {
        this.pool = pool;
        pool.setRejectedExecutionHandler(
                new CountingRejectionHandler(pool.getRejectedExecutionHandler()));
    }

    @Override
    public void shutdown() {
        this.pool.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return pool.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return pool.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return pool.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return pool.awaitTermination(timeout,unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return pool.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return pool.submit(task,result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return pool.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return pool.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return pool.invokeAll(tasks,timeout,unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return pool.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return pool.invokeAny(tasks,timeout,unit);
    }

    @Override
    public void execute(Runnable command) {
        pool.execute(command);
    }

    @Override
    public int getCorePoolSize(){
        return pool.getCorePoolSize();
    }

    @Override
    public void setCorePoolSize(int corePoolSize) {
        pool.setCorePoolSize(corePoolSize);
    }

    @Override
    public int getMaximumPoolSize(){
        return pool.getMaximumPoolSize();
    }

    @Override
    public void setMaximumPoolSize(int maximumPoolSize) {
        pool.setMaximumPoolSize(maximumPoolSize);
    }

    @Override
    public long getThreadKeepAliveTime(){
        return pool.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }
    @Override
    public void setThreadKeepAliveTime(long timeMs) {
        pool.setKeepAliveTime(timeMs,TimeUnit.MILLISECONDS);
    }

    @Override
    public int getCurrentPoolSize() {
        return pool.getPoolSize();
    }

    @Override
    public int getCurrentlyExecutingThreads() {
        return pool.getActiveCount();
    }

    @Override
    public int getLargestPoolSize() {
        return pool.getLargestPoolSize();
    }

    @Override
    public long getTotalScheduledTasks() {
        return pool.getTaskCount();
    }

    @Override
    public long getTotalCompletedTasks() {
        return pool.getCompletedTaskCount();
    }

    @Override
    public int getCurrentlyAvailableThreads(){
        int maxThreads = pool.getMaximumPoolSize();
        if(maxThreads ==Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return maxThreads-pool.getActiveCount();
    }

    @Override
    public int getPendingTasks(){
        return (int)(pool.getTaskCount() - pool.getCompletedTaskCount());
    }

    @Override
    public long getTotalRejectedTasks(){
        return totalRejected.get();
    }

    private class CountingRejectionHandler implements RejectedExecutionHandler {
        private final RejectedExecutionHandler delegate;
        public CountingRejectionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.delegate = rejectedExecutionHandler;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            totalRejected.incrementAndGet();
            if(delegate!=null)
                delegate.rejectedExecution(r,executor);
        }
    }
}
