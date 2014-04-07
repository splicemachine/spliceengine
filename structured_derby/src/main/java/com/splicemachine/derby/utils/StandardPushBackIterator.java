package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * @author P Trolard
 *         Date: 25/03/2014
 */
public class StandardPushBackIterator<T> implements StandardIterator<T> {
    private final StandardIterator<T> iterator;
    private T pushedBack;

    public StandardPushBackIterator(StandardIterator<T> iterator){
        this.iterator = iterator;
    }

    @Override
    public T next(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if (pushedBack != null) {
            T next = pushedBack;
            pushedBack = null;
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
        if (pushedBack != null) {
            throw new RuntimeException("Cannot push back multiple values.");
        }
        pushedBack = value;
    }
}
