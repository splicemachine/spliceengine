package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * Created by jyuan on 7/27/14.
 */
public class PartitionAwarePushBackIterator<T> implements PartitionAwareIterator<T>{

    private final PartitionAwareIterator<T> iterator;
    private final T EMPTY = (T) new Object();
    private T pushedBack = EMPTY;

    public PartitionAwarePushBackIterator(PartitionAwareIterator<T> iterator){
        this.iterator = iterator;
    }

    @Override
    public T next(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if (pushedBack != EMPTY) {
            T next = pushedBack;
            pushedBack = EMPTY;
            return next;
        }
        return iterator.next(ctx);
    }

    @Override
    public void open() throws StandardException, IOException {
        iterator.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        iterator.close();
    }

    public void pushBack(T value){
        if (pushedBack != EMPTY) {
            throw new RuntimeException("Cannot push back multiple values.");
        }
        pushedBack = value;
    }

    @Override
    public byte[] getPartition() {
        return iterator.getPartition();
    }
}
