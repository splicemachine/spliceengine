package com.splicemachine.hbase.writer;

import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public interface CallBuffer<E> {
    /**
     * Adds an entry to the buffer, flushing the buffer if it's full.
     *
     * @param element the element to add.
     * @throws Exception if the buffer is flushed and something goes wrong during the flush operation.
     */
    void add(E element) throws Exception;

    /**
     * Bulk adds multiple elements to the buffer, flushing it if it's full.
     *
     * @param elements the elements to add
     * @throws Exception if the buffer is flushed, and then something goes wrong during
     * the buffer flush operation.
     */
    void addAll(E[] elements) throws Exception;

    /**
     * Bulk adds multiple elements to the buffer, flushing it if it's full.
     *
     * @param elements the elements to add
     * @throws Exception if the buffer is flushed, and then something goes wrong during
     * the buffer flush operation.
     */
    void addAll(Collection<? extends E> elements) throws Exception;

    /**
     * Flush the buffer.
     *
     * @throws Exception if something goes wrong during the flush operation.
     */
    void flushBuffer()  throws Exception;

    /**
     * Close the buffer.
     *
     * @throws Exception if something goes wrong during the buffer flush operation.
     */
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
         * Note: the entries list is decoupled from the buffer itself. That is,
         * it represents the state of the buffer when {@code bufferFlushed} is called,
         * entries added to the buffer after that will not be present in the entries list.
         *
         * @param entries the entries to operate against (the contents of the entire buffer).
         * @throws Exception if the operation fails in some say.
         */
        void bufferFlushed(List<T> entries,CallBuffer<T> source) throws Exception;
    }
}
