/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.context;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the write context
 *
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public interface WriteContext {

    /**
     * Do not run the following KVPair
     */
    void notRun(KVPair mutation);

    /**
     * Send KVPair upstream to be processes by possible handlers.
     */
    void sendUpstream(KVPair mutation);

    /**
     * Fail the following KVPair and put them into the WriteResult
     */
    void failed(KVPair put, WriteResult mutationResult);

    /**
     * Mark KVPair as successful
     */
    void success(KVPair put);

    void result(KVPair put, WriteResult result);

    /**
     * Update an existing result when you don't have the KVPair, only the mutation's rowKey.
     */
    void result(byte[] rowKey, WriteResult result);

    /**
     * Retrieve the region from the context
     */
    Partition getRegion();

    /**
     * Retrieve a remote partition based on the index bytes[] name
     */
    Partition remotePartition(byte[] indexConglomBytes) throws IOException;

    /**
     * Retrieve the sharedWriteBuffer for the index upsert handler
     */
    CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
                                            ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                            int maxSize,
                                            boolean useAsyncWriteBuffers,
                                            TxnView txn) throws Exception;

    /**
     * Retrieve the coprocessor environment
     */
    ServerControl getCoprocessorEnvironment();

    /**
     * Flush
     */
    void flush() throws IOException;

    /**
     * Close
     */
    Map<KVPair, WriteResult> close() throws IOException;

    Map<KVPair,WriteResult> currentResults();
    /**
     * CanRun
     */
    boolean canRun(KVPair input);

    /**
     * Retrieve Transaction
     */
    TxnView getTxn();

    boolean skipIndexWrites();

    TransactionalRegion txnRegion();

    PipelineExceptionFactory exceptionFactory();

}