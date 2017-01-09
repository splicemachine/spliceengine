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

package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.SharedWriteConfiguration;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;

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
    private ObjectObjectOpenHashMap<byte[], CallBuffer<Record>> sharedCallBufferMap = new ObjectObjectOpenHashMap<>();
    private final WriteCoordinator writerPool;
    private final PartitionFactory partitionFactory;

    public SharedCallBufferFactory(WriteCoordinator writerPool){
        this.writerPool=writerPool;
        this.partitionFactory = writerPool.getPartitionFactory();
    }

    public CallBuffer<Record> getWriteBuffer(byte[] conglomBytes,
                                             WriteContext context,
                                             ObjectObjectOpenHashMap<Record, Record> indexToMainMutationMap,
                                             int maxSize,
                                             boolean useAsyncWriteBuffers,
                                             Txn txn) throws Exception {

        CallBuffer<Record> writeBuffer = sharedCallBufferMap.get(conglomBytes);
        if (writeBuffer == null) {
            writeBuffer = createKvPairCallBuffer(conglomBytes, context, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
        } else {
            ((SharedPreFlushHook) writeBuffer.getPreFlushHook()).registerContext(context, indexToMainMutationMap);
            writeBuffer.getWriteConfiguration().registerContext(context, indexToMainMutationMap);
        }
        return writeBuffer;
    }

    private CallBuffer<Record> createKvPairCallBuffer(byte[] conglomBytes,
                                                      WriteContext context,
                                                      ObjectObjectOpenHashMap<Record, Record> indexToMainMutationMap,
                                                      int maxSize,
                                                      boolean useAsyncWriteBuffers,
                                                      Txn txn) throws IOException{
        SharedPreFlushHook hook = new SharedPreFlushHook();
        WriteConfiguration writeConfiguration=writerPool.defaultWriteConfiguration();
        SharedWriteConfiguration wc = new SharedWriteConfiguration(writeConfiguration.getMaximumRetries(),
                writeConfiguration.getPause(),
                writeConfiguration.getExceptionFactory());
        hook.registerContext(context, indexToMainMutationMap);
        wc.registerContext(context, indexToMainMutationMap);
        CallBuffer<Record> writeBuffer;
        if (useAsyncWriteBuffers) {
            writeBuffer = writerPool.writeBuffer(partitionFactory.getTable(conglomBytes), txn, hook, wc);
        } else {
            writeBuffer = writerPool.synchronousWriteBuffer(partitionFactory.getTable(conglomBytes), txn, hook, wc, maxSize);
        }
        sharedCallBufferMap.put(conglomBytes, writeBuffer);
        return writeBuffer;
    }
}
