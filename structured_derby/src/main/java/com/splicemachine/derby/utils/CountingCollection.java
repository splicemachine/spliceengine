package com.splicemachine.derby.utils;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public class CountingCollection<E> extends AbstractCollection<E> {
    private final Collection<E> delegate;
    private final AtomicLong addedCount = new AtomicLong(0l);
    private final AtomicLong removedCount = new AtomicLong(0l);

    public CountingCollection(Collection<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Iterator<E> iterator() {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public boolean add(E e) {
        boolean added = delegate.add(e);
        if(added)
            addedCount.incrementAndGet();
        return added;
    }

    @Override
    public boolean remove(Object o) {
        boolean removed = delegate.remove(o);
        if(removed)
            removedCount.incrementAndGet();
        return removed;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean added = delegate.addAll(c);
        if(added)
            removedCount.addAndGet(c.size());
        return added;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean removed = delegate.removeAll(c);
        if(removed)
            removedCount.addAndGet(c.size());
        return removed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        int currentSize = delegate.size();
        boolean retained = delegate.retainAll(c);
        if(retained){
            removedCount.addAndGet(currentSize-c.size());
        }
        return retained;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

}
