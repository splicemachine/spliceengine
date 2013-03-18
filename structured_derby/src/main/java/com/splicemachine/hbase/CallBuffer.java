package com.splicemachine.hbase;

import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public interface CallBuffer<E> {
    void add(E element) throws Exception;

    void addAll(E[] elements) throws Exception;

    void addAll(Collection<? extends E> elements) throws Exception;

    void flushBuffer()  throws Exception;

    void close() throws Exception;

    /**
     * Listener for Buffer filling Events
     *
     * @param <T> the type of the entry in the buffer
     */
    public interface Listener<T>{
        /**
         * Determine the heap size of the Buffer.
         *
         * @param element the element to get the heap size for
         * @return the heap size of the specified element
         */
        long heapSize(T element);

        /**
         * The Buffer has been flushed into the entry list. Time to
         * do the expensive operation.
         *
         * @param entries the entries to operate against (the contents of the entire buffer).
         * @throws Exception if the operation fails in some say.
         */
        void bufferFlushed(List<T> entries) throws Exception;
    }
}
