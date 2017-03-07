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

    boolean skipConflictDetection();

    TransactionalRegion txnRegion();

    PipelineExceptionFactory exceptionFactory();

}