package com.splicemachine.concurrent;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class CancellableCompletionService<V> implements CompletionService<V>{
    private final CompletionService<V> delegate;
    private final List<Future<V>> futures;

    public CancellableCompletionService(CompletionService<V> delegate){
        this.delegate=delegate;
        this.futures = new LinkedList<>();
    }

    @Override
    public Future<V> submit(Callable<V> task){
        Future<V> submit=delegate.submit(task);
        futures.add(submit);
        return submit;
    }

    @Override
    public Future<V> submit(Runnable task,V result){
        Future<V> submit=delegate.submit(task,result);
        futures.add(submit);
        return submit;
    }

    @Override
    public Future<V> take() throws InterruptedException{
        return delegate.take();
    }

    @Override
    public Future<V> poll(){
        return delegate.poll();
    }

    @Override
    public Future<V> poll(long timeout,TimeUnit unit) throws InterruptedException{
        return delegate.poll(timeout,unit);
    }

    public void cancellAll(boolean interruptIfRunning){
        for(Future<V> future:futures){
            future.cancel(interruptIfRunning);
        }
    }
}
