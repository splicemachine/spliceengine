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

package com.splicemachine.concurrent;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 10/23/13
 */
public class KeyedCompletionService<K,V>{
    private final ExecutorService executorService;
    private final BlockingQueue<KeyedFuture<K,V>> queue;

    public KeyedCompletionService(ExecutorService executorService) {
        this.executorService = executorService;
        this.queue = new LinkedBlockingQueue<KeyedFuture<K, V>>();
    }

    public KeyedFuture<K,V> submit(K key, Callable<V> callable){
        KeyedQueueFuture<K,V> kqf = newTaskFor(key,callable);
        executorService.execute(kqf);
        return kqf;
    }

    private KeyedQueueFuture<K,V> newTaskFor(K key, Callable<V> callable) {
        return new KeyedQueueFuture<K,V>(callable,key,queue);
    }

    public KeyedFuture<K,V> take() throws InterruptedException{
        return queue.take();
    }

    private static class KeyedQueueFuture<K,V> extends FutureTask<V> implements KeyedFuture<K,V>{
        private final K key;
        private final BlockingQueue<KeyedFuture<K,V>> completionQueue;

        private KeyedQueueFuture(Callable<V> callable, K key,BlockingQueue<KeyedFuture<K,V>> completionQueue) {
            super(callable);
            this.key = key;
            this.completionQueue = completionQueue;
        }

        @Override
        protected void done() {
            completionQueue.add(this);
        }

        @Override
        public K getKey() {
            return key;
        }
    }
}
