package com.splicemachine.pipeline.api;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;

public interface WriteBufferFactory {

	CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
             WriteContext context,
             ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap,
             int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception;	
}
