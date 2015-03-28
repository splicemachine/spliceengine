package com.splicemachine.pipeline.api;

import com.carrotsearch.hppc.ObjectArrayList;

import java.util.Collection;

/**
 * This is an interface to a data structure to buffer (or queue) HBase RPC calls.
 * Technically, the calls don't need to be HBase RPC calls.  That just happens to be the interface that was first implemented.
 *
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
     *                   the buffer flush operation.
     */
    void addAll(E[] elements) throws Exception;

    PreFlushHook getPreFlushHook();

    WriteConfiguration getWriteConfiguration();

    /**
     * Bulk adds multiple elements to the buffer, flushing it if it's full.
     *
     * @param elements the elements to add
     * @throws Exception if the buffer is flushed, and then something goes wrong during
     *                   the buffer flush operation.
     */
    void addAll(Iterable<E> elements) throws Exception;

    /**
     * Flush the buffer.
     *
     * @throws Exception if something goes wrong during the flush operation.
     */
    void flushBuffer() throws Exception;

    /**
     * Close the buffer.
     *
     * @throws Exception if something goes wrong during the buffer flush operation.
     */
    void close() throws Exception;

}
