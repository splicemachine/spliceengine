package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteBufferFactory;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.writeconfiguration.IndexSharedWriteConfiguration;
import com.splicemachine.si.api.TxnView;

public class IndexWriteBufferFactory implements WriteBufferFactory {
    /*This class is NOT thread safe*/
    private ObjectObjectOpenHashMap<byte[],CallBuffer<KVPair>> sharedCallBufferMap;

    public IndexWriteBufferFactory() {
    	sharedCallBufferMap = new ObjectObjectOpenHashMap<>();
    }
    @Override
    public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                             WriteContext context,
                                             ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap,
                                             int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception {
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
		}
}
