package com.splicemachine.derby.impl.load;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Scott Fines
 *         Created on: 8/28/13
 */
public class BoundedConcurrentLinkedQueue<E> implements BlockingQueue<E> {
    private final ConcurrentLinkedQueue<E> delegate;
    private final int maxSize;
    private final AtomicInteger currentSize = new AtomicInteger(0);

    public BoundedConcurrentLinkedQueue(int maxSize) {
        this.maxSize = maxSize;
        this.delegate = new ConcurrentLinkedQueue<E>();
    }

    @Override
    public boolean add(E e) {
        if(!offer(e))
            throw new IllegalStateException();
        return true;
    }

    @Override
    public boolean offer(E e) {
        int size;
        boolean success;
        do{
            size = currentSize.get();
            if(size >=maxSize)
                return false;
            success = currentSize.compareAndSet(size,size+1);
        }while(!success);

        return delegate.add(e);
    }

    @Override
    public E remove() {
        E item = poll();
        if(item==null)
            throw new NoSuchElementException();
        return item;
    }

    @Override
    public E poll() {
        E item = delegate.poll();
        if(item!=null)
            currentSize.decrementAndGet();
        return item;
    }

    @Override
    public E element() {
        return delegate.element();
    }

    @Override
    public E peek() {
        return delegate.peek();
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e,Long.MAX_VALUE,TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        int spinCount=200;
        long nanosRemaining = unit.toNanos(timeout);
        boolean success = false;
        int size;
        long start = System.nanoTime();
        do{
            size= currentSize.get();
            if(size>=maxSize){
                if(spinCount>100)
                    spinCount--;
//                else if(spinCount>0)
//                    Thread.yield();
                else
                    LockSupport.parkNanos(1L);
            }else{
                success = currentSize.compareAndSet(size,size+1);
            }
            long stop = System.nanoTime();
            nanosRemaining-=(stop-start);
            start = stop;
        }while(nanosRemaining>0 &&!success);

        return success && delegate.offer(e);

    }

    @Override
    public E take() throws InterruptedException {
        return poll(Long.MAX_VALUE,TimeUnit.DAYS);
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        int spintCount=200;
        long nanosRemaining = unit.toNanos(timeout);

        int size;
        boolean success = false;
        long start = System.nanoTime();
        do{
            size = currentSize.get();
            if(size<=0){
                if(spintCount>100)
                    spintCount--;
//                else if(spintCount>0){
//                    spintCount--;
//                    Thread.yield();
//                }
                else
                    LockSupport.parkNanos(1L);
            }else{
                success = currentSize.compareAndSet(size,size-1);
            }
            long stop = System.nanoTime();
            nanosRemaining-=(stop-start);
            start = stop;
        }while(nanosRemaining>0 && !success);

        return !success ? null : delegate.poll();
    }

    @Override
    public int remainingCapacity() {
        return maxSize-currentSize.get();
    }

    @Override
    public boolean remove(Object o) {
        if(delegate.remove(o)){
            currentSize.decrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public int size() {
        return currentSize.get();
    }

    @Override
    public boolean isEmpty() {
        return currentSize.get()<=0;
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override public boolean addAll(Collection<? extends E> c) { throw new UnsupportedOperationException(); }
    @Override public boolean removeAll(Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override public boolean retainAll(Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override public void clear() { throw new UnsupportedOperationException(); }
    @Override public Iterator<E> iterator() { throw new UnsupportedOperationException(); }
    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }
    @Override public <T> T[] toArray(T[] a) { throw new UnsupportedOperationException(); }
    @Override public int drainTo(Collection<? super E> c) { throw new UnsupportedOperationException(); }
    @Override public int drainTo(Collection<? super E> c, int maxElements) { throw new UnsupportedOperationException(); }
}
