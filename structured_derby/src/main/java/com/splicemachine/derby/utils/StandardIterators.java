package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;
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

    public static <T> StandardIteratorIterator<T> asIter(StandardIterator<T> stdIt){
        return new StandardIteratorIterator<T>(stdIt);
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

    public static class StandardIteratorIterator<T> implements Iterator<T>{
        private final StandardIterator<T> delegate;
        private boolean needToCheckDelegate = true;
        private StandardException se;
        private IOException ioe;
        private T value;

        private StandardIteratorIterator(StandardIterator<T> delegate){
            this.delegate = delegate;
        }

        public boolean hasException(){
            return se != null || ioe != null;
        }

        public void throwExceptions() throws StandardException, IOException {
            if (se != null){
                throw se;
            }
            if (ioe != null){
                throw ioe;
            }
        }

        @Override
        public boolean hasNext(){
            if (needToCheckDelegate && !hasException()){
                try {
                    value = delegate.next();
                    needToCheckDelegate = false;
                } catch (StandardException e){
                    se = e;
                } catch (IOException e){
                    ioe = e;
                }
            }
            return !hasException() && value != null;
        }

        @Override
        public T next() {
            if (hasException()){
                return null;
            }
            needToCheckDelegate = true;
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
