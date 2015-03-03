package com.splicemachine.derby.utils;

import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;

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

    public boolean test(SpliceRuntimeContext ctx) throws IOException, StandardException {
        iterator.open();
        T row = this.next(ctx);
        if (row != null) {
            // frame source works, reset
            this.pushBack(row);
            return true;
        }
        return false;
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
