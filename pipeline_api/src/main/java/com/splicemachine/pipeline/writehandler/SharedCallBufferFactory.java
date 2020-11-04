/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.config.RollforwardWriteConfiguration;
import com.splicemachine.pipeline.config.UnsafeWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.SharedWriteConfiguration;
import com.splicemachine.si.api.txn.TxnView;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/*
 * When performing the region-side processing of a base-table BulkWrite, writing to N regions on a single region server,
 * there will be N WriteContexts and N IndexWriteHandlers. We don't want N CallBuffers for remotely sending the index
 * writes. Thus this class.
 *
 * This class is NOT thread safe.
 */
@NotThreadSafe
public class SharedCallBufferFactory{

    /* conglomerateId to CallBuffer */
    private ObjectObjectHashMap<byte[], CallBuffer<KVPair>> sharedCallBufferMap = new ObjectObjectHashMap<>();
    private final WriteCoordinator writerPool;
    private final PartitionFactory partitionFactory;

    public SharedCallBufferFactory(WriteCoordinator writerPool){
        this.writerPool=writerPool;
        this.partitionFactory = writerPool.getPartitionFactory();
    }

    public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                             WriteContext context,
                                             ObjectObjectHashMap<KVPair, KVPair> indexToMainMutationMap,
                                             int maxSize,
                                             boolean useAsyncWriteBuffers,
                                             TxnView txn, byte[] token) throws Exception {

        CallBuffer<KVPair> writeBuffer = sharedCallBufferMap.get(conglomBytes);
        if (writeBuffer == null) {
            writeBuffer = createKvPairCallBuffer(conglomBytes, context, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn, token);
        } else {
            ((SharedPreFlushHook) writeBuffer.getPreFlushHook()).registerContext(context, indexToMainMutationMap);
            writeBuffer.getWriteConfiguration().registerContext(context, indexToMainMutationMap);
        }
        return writeBuffer;
    }

    private CallBuffer<KVPair> createKvPairCallBuffer(byte[] conglomBytes,
                                                      WriteContext context,
                                                      ObjectObjectHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                      int maxSize,
                                                      boolean useAsyncWriteBuffers,
                                                      TxnView txn, byte[] token) throws IOException{
        SharedPreFlushHook hook = new SharedPreFlushHook();
        WriteConfiguration writeConfiguration=writerPool.defaultWriteConfiguration();
        WriteConfiguration wc = new SharedWriteConfiguration(writeConfiguration.getMaximumRetries(),
                writeConfiguration.getPause(),
                writeConfiguration.getExceptionFactory());
        if (context.skipConflictDetection() || context.skipWAL()) {
            wc = new UnsafeWriteConfiguration(wc, context.skipConflictDetection(), context.skipWAL());
        }
        if (context.rollforward()) {
            wc = new RollforwardWriteConfiguration(wc);
        }
        hook.registerContext(context, indexToMainMutationMap);
        wc.registerContext(context, indexToMainMutationMap);
        CallBuffer<KVPair> writeBuffer;
        if (useAsyncWriteBuffers) {
            writeBuffer = writerPool.writeBuffer(partitionFactory.getTable(conglomBytes), txn,token, hook, wc, false);
        } else {
            writeBuffer = writerPool.synchronousWriteBuffer(partitionFactory.getTable(conglomBytes), txn, token, hook, wc, maxSize, false);
        }
        sharedCallBufferMap.put(conglomBytes, writeBuffer);
        return writeBuffer;
    }
}
