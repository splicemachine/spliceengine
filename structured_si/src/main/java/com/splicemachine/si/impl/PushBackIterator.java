package com.splicemachine.si.impl;

import java.util.Iterator;

/**
 * Extends the Iterator interface to include a function to pushBack(...) a value after it has been read. This effectively
 * provides a peek mechanism for the iterator.
 */
public class PushBackIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;

    private T pushedBack;

    public PushBackIterator(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if (pushedBack == null) {
            return iterator.hasNext();
        } else {
            return true;
        }
    }

    @Override
    public T next() {
        if (pushedBack == null) {
            return iterator.next();
        } else {
            T result = pushedBack;
            pushedBack = null;
            return result;
        }
    }

    @Override
    public void remove() {
        if (pushedBack == null) {
            iterator.remove();
        } else {
            pushedBack = null;
        }
    }

    public void pushBack(T value) {
        if (pushedBack != null) {
            throw new RuntimeException("Cannot push back multiple values.");
        }
        this.pushedBack = value;
    }
}
