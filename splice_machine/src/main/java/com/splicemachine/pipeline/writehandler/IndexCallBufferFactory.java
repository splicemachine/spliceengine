package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.pipeline.writeconfiguration.IndexSharedWriteConfiguration;
import com.splicemachine.si.api.TxnView;

/*
 * When performing the region-side processing of a base-table BulkWrite, writing to N regions on a single region server,
 * there will be N WriteContexts and N IndexWriteHandlers. We don't want N CallBuffers for remotely sending the index
 * writes. Thus this class.
 *
 * This class is NOT thread safe.
 */
public class IndexCallBufferFactory {

    /* conglomerateId to CallBuffer */
    private ObjectObjectOpenHashMap<byte[], CallBuffer<KVPair>> sharedCallBufferMap = new ObjectObjectOpenHashMap<>();

    public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                             WriteContext context,
                                             ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                             int maxSize,
                                             boolean useAsyncWriteBuffers,
                                             TxnView txn) throws Exception {

        CallBuffer<KVPair> writeBuffer = sharedCallBufferMap.get(conglomBytes);
        if (writeBuffer == null) {
            writeBuffer = createKvPairCallBuffer(conglomBytes, context, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
        } else {
            ((IndexSharedPreFlushHook) writeBuffer.getPreFlushHook()).registerContext(context, indexToMainMutationMap);
            writeBuffer.getWriteConfiguration().registerContext(context, indexToMainMutationMap);
        }
        return writeBuffer;
    }

    private CallBuffer<KVPair> createKvPairCallBuffer(byte[] conglomBytes,
                                                      WriteContext context,
                                                      ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                      int maxSize,
                                                      boolean useAsyncWriteBuffers,
                                                      TxnView txn) {
        IndexSharedPreFlushHook hook = new IndexSharedPreFlushHook();
        IndexSharedWriteConfiguration wc = new IndexSharedWriteConfiguration();
        hook.registerContext(context, indexToMainMutationMap);
        wc.registerContext(context, indexToMainMutationMap);
        CallBuffer<KVPair> writeBuffer;
        WriteCoordinator writerPool = SpliceDriver.driver().getTableWriter();
        if (useAsyncWriteBuffers) {
            writeBuffer = writerPool.writeBuffer(conglomBytes, txn, hook, wc);
        } else {
            writeBuffer = writerPool.synchronousWriteBuffer(conglomBytes, txn, hook, wc, maxSize);
        }
        sharedCallBufferMap.put(conglomBytes, writeBuffer);
        return writeBuffer;
    }
}
