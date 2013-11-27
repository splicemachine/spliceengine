package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class StandardIterators {

    private StandardIterators(){}

    public static <T> StandardIterator<T> wrap(Iterable<T> data){
        return new IteratorStandardIterator<T>(data.iterator());
    }

    public static StandardIterator<ExecRow> wrap(NoPutResultSet NPRS){
        return new ResultSetStandardIterator(NPRS);
    }

    public static <T> StandardIterator<T> wrap(Callable<T> callable){
        return new CallableStandardIterator<T>(callable);
    }

    public static <T> StandardIteratorIterator<T> asIter(StandardIterator<T> stdIt){
        return new StandardIteratorIterator<T>(stdIt);
    }

    private static class IteratorStandardIterator<T> implements StandardIterator<T>{
        private final Iterator<T> delegate;

        private IteratorStandardIterator(Iterator<T> delegate) {
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

    private static class ResultSetStandardIterator implements StandardIterator<ExecRow>{
        private final NoPutResultSet noPut;

        private ResultSetStandardIterator(NoPutResultSet noPut){
            this.noPut = noPut;
        }

        @Override
        public void open() throws StandardException, IOException {
            noPut.open();
            noPut.openCore();
        }

        @Override
        public ExecRow next() throws StandardException, IOException {
            return noPut.getNextRowCore();
        }

        @Override
        public void close() throws StandardException, IOException {
            noPut.close();
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

        public void open() throws StandardException, IOException { delegate.open(); }
        public void close() throws StandardException, IOException { delegate.close(); }

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

    public static class CallableIterator<T> implements Iterator<T>{
        Callable<T> callable;
        private T element;

        public CallableIterator(Callable<T> callable){
            this.callable = callable;
        }

        @Override
        public boolean hasNext() {
            if (element == null){
                try {
                    element = callable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return element != null;
        }

        @Override
        public T next() {
            T value = element;
            element = null;
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static class CallableStandardIterator<T> implements StandardIterator<T>{
        Callable<T> callable;

        public CallableStandardIterator(Callable<T> callable){
            this.callable = callable;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public T next() throws StandardException, IOException {
            try {
                return callable.call();
            } catch (StandardException se){
                throw se;
            } catch (IOException ioe){
                throw ioe;
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        }
    }
}
