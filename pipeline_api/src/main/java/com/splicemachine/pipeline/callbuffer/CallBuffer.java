/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Partition;

/**
 * This is an interface to a data structure to buffer (or queue) HBase RPC calls.
 * Technically, the calls don't need to be HBase RPC calls.  That just happens to be the interface that was first implemented.
 *
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public interface CallBuffer<E> extends AutoCloseable {

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
     * Flush buffered writes but don't necessarily wait them (network calls) to complete.
     *
     * @throws Exception if something goes wrong during the flush operation.
     */
    void flushBuffer() throws Exception;

    /**
     * Flush buffered writes and wait for them to complete.  Use this version of flush to be sure that the writes are
     * persisted remotely (with constraints checked, etc) before the calling thread returns.
     *
     * @throws Exception if constraint violation or other problem writing
     */
    void flushBufferAndWait() throws Exception;

    /**
     * Close the buffer.
     *
     * @throws Exception if something goes wrong during the buffer flush operation.
     */
    void close() throws Exception;


    /**
     *
     * Retrieve the current transaction.
     *
     */
    Txn getTxn();

    Partition destinationPartition();

    /**
     * @return the last KVPair added to this buffer. This is an optional method, and may throw UnsupportedOperationException.
     */
    E lastElement();
}
