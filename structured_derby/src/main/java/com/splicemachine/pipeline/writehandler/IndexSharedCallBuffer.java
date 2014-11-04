package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.SharedCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.writeconfiguration.IndexSharedWriteConfiguration;
import com.splicemachine.si.api.TxnView;

public class IndexSharedCallBuffer implements SharedCallBuffer{
    private ObjectObjectOpenHashMap<byte[],CallBuffer<KVPair>> sharedCallBufferMap;

    public IndexSharedCallBuffer() {
    	sharedCallBufferMap = new ObjectObjectOpenHashMap<byte[],CallBuffer<KVPair>>();
    }
    @Override
    public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                             WriteContext context,
                                             ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap,
                                             int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception {
    	try {
    	CallBuffer<KVPair> writeBuffer = sharedCallBufferMap.get(conglomBytes);
    	if (writeBuffer == null) {
    		IndexSharedPreFlushHook hook = new IndexSharedPreFlushHook();
    		IndexSharedWriteConfiguration wc = new IndexSharedWriteConfiguration();
    		hook.registerContext(context, indexToMainMutationMap);
    		wc.registerContext(context, indexToMainMutationMap);
            if(useAsyncWriteBuffers)
            	writeBuffer = SpliceDriver.driver().getWriterPool().writeBuffer(conglomBytes,txn, hook, wc);
            else
            	writeBuffer = SpliceDriver.driver().getWriterPool().synchronousWriteBuffer(conglomBytes,txn,hook, wc,maxSize);
            sharedCallBufferMap.put(conglomBytes, writeBuffer);
    	} else { 
    		((IndexSharedPreFlushHook)writeBuffer.getPreFlushHook()).registerContext(context, indexToMainMutationMap);
    		writeBuffer.getWriteConfiguration().registerContext(context, indexToMainMutationMap);
    	}
    	return writeBuffer;
    	} catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    }
}
