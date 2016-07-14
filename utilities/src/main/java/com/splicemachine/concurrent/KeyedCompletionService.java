/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
