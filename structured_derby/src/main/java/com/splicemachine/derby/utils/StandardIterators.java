package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class StandardIterators {

    private StandardIterators(){}

    public static <T> StandardIterator<T> wrap(Iterable<T> data){
        return new IteratorIterator<T>(data.iterator());
    }

    private static class IteratorIterator<T> implements StandardIterator<T>{
        private final Iterator<T> delegate;

        private IteratorIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override public void open() throws StandardException, IOException { }//no-op
        @Override public void close() throws StandardException, IOException { } //no-op

        @Override
        public T next() throws StandardException, IOException {
            if(!delegate.hasNext())
                return null;
            return delegate.next();
        }
    }
}
