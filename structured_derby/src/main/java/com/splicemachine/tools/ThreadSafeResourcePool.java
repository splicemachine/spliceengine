package com.splicemachine.tools;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resource Pool for managing cached references to a commonly used Resource which
 * is Thread-safe.
 *
 * This implementation uses non-blocking, optimistic concurrency to improve performance.
 *
 * Warning: If your resource is not thread-safe, then this implementation WILL NOT BE SAFE.
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class ThreadSafeResourcePool<E,K extends ResourcePool.Key> implements ResourcePool<E,K> {
    private final Generator<E,K> generator;
    private final ConcurrentMap<Key,Counter<E>> pool = new ConcurrentHashMap<Key, Counter<E>>();

    private final boolean removeWhenUnused;

    public ThreadSafeResourcePool(Generator<E,K> generator) {
        this(generator,true);
    }

    public ThreadSafeResourcePool(Generator<E,K> generator, boolean removeWhenUnused) {
        this.generator = generator;
        this.removeWhenUnused = removeWhenUnused;
    }

    @Override
    public E get(K key) throws Exception{
        Counter<E> cachedEntry = pool.get(key);
        if(cachedEntry==null||cachedEntry.refCount.getAndIncrement()<=0){
            E next = generator.makeNew(key);
            Counter<E> counter = new Counter<E>(next);
            while(true){
                Counter<E> retCount = pool.putIfAbsent(key,counter);
                if(retCount==null) return counter.ref;
                else if(retCount.refCount.getAndIncrement()>0){
                    generator.close(counter.ref);
                    return retCount.ref;
                }
            }
        }
        return cachedEntry.ref;
    }

    @Override
    public void release(K key) throws Exception {
        Counter<E> cachedEntry = pool.get(key);
        if(cachedEntry==null) return; //nothing to do

        int refCount = cachedEntry.refCount.decrementAndGet();
        if(refCount<=0 && removeWhenUnused){
            //need to close it
            pool.remove(key);
            generator.close(cachedEntry.ref);
        }
    }

    private static class Counter<T> {
        private final AtomicInteger refCount;
        private final T ref;

        private Counter(T ref) {
            this.ref = ref;

            this.refCount = new AtomicInteger(1);
        }

    }
}
