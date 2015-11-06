package com.splicemachine.derby.stream.iterator;

import com.splicemachine.derby.stream.iapi.PartitionAwareIterator;

/**
 * Created by jleach on 4/28/15.
 */
public class PartitionAwarePushBackIterator<T> implements PartitionAwareIterator<T> {
    private final PartitionAwareIterator<T> iterator;
    private final T EMPTY = (T) new Object();
    private T pushedBack = EMPTY;

    public PartitionAwarePushBackIterator(PartitionAwareIterator<T> iterator){
        this.iterator = iterator;
    }

    @Override
    public T next() {
        if (pushedBack != EMPTY) {
            T next = pushedBack;
            pushedBack = EMPTY;
            return next;
        }
        return iterator.next();
    }

    public void pushBack(T value){
        if (pushedBack != EMPTY) {
            throw new RuntimeException("Cannot push back multiple values.");
        }
        pushedBack = value;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public byte[] getPartition() {
        return iterator.getPartition();
    }
}

