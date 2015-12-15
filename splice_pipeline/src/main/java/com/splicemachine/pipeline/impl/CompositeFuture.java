package com.splicemachine.pipeline.impl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;

public class CompositeFuture<T> implements Future<T>{
    private final List<Future<T>> futures = Lists.newArrayList();
    private volatile boolean cancelled = false;

    public void add(Future<T> future){
        this.futures.add(future);
    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if(cancelled)
             return true;

        for(Future<T> future:futures){
            cancelled = cancelled && future.cancel(mayInterruptIfRunning);
        }
        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        for(Future<T> future:futures){
            if(!future.isDone()) return false;
        }
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        //return the last entry
        T next = null;
        for(Future<T> future:futures){
            next = future.get();
        }
        return next;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        //return the last entry
        T next = null;
        long timeRemaining =unit.toNanos(timeout);
        for(Future<T> future:futures){
            if(timeRemaining<=0) return next;
            long start = System.nanoTime();
            next = future.get(timeout,unit);
            timeRemaining -=System.nanoTime()-start;
        }
        return next;
    }
}
