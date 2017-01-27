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
