package com.splicemachine.pipeline.writecontext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TxnView;

	public class WriteNode implements WriteContext{
        public WriteHandler handler;
        public WriteNode next;
        private PipelineWriteContext pipelineWriteContext;

        public WriteNode(WriteHandler handler, PipelineWriteContext pipelineWriteContext){ 
        	this.handler = handler; 
        	this.pipelineWriteContext = pipelineWriteContext;
        }

        @Override
        public void notRun(KVPair mutation) {
        	pipelineWriteContext.notRun(mutation);
        }

        @Override
        public void sendUpstream(KVPair mutation) {
            if(next!=null)
                next.handler.next(mutation,next);
        }

        @Override
        public void failed(KVPair put, WriteResult mutationResult) {
        	pipelineWriteContext.failed(put,mutationResult);
        }

        @Override
        public void success(KVPair put) {
        	pipelineWriteContext.success(put);
        }

        @Override
        public void result(KVPair put, WriteResult result) {
        	pipelineWriteContext.result(put,result);
        }

        @Override
        public HRegion getRegion() {
            return getCoprocessorEnvironment().getRegion();
        }

        @Override
        public HTableInterface getHTable(byte[] indexConglomBytes) {
            return pipelineWriteContext.getHTable(indexConglomBytes);
        }

		@Override
		public CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
				ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
				int maxSize, boolean useAsyncWriteBuffers, TxnView txn)
				throws Exception {
			return pipelineWriteContext.getSharedWriteBuffer(conglomBytes, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
		}			

        @Override
        public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
            return pipelineWriteContext.getCoprocessorEnvironment();
        }

        @Override
        public void flush() throws IOException {
        		handler.flush(this);
        }

        @Override
        public Map<KVPair,WriteResult> close() throws IOException {
	        handler.close(this);
	        return null; //ignored
        }

        @Override
        public boolean canRun(KVPair input) {
            return pipelineWriteContext.canRun(input);
        }

		@Override
		public long getTransactionTimestamp() {
			return pipelineWriteContext.getTransactionTimestamp();
		}

		@Override
		public void sendUpstream(List<KVPair> mutation) {
			throw new RuntimeException("Not Supported");
		}

		@Override
		public TxnView getTxn() {
			// TODO Auto-generated method stub
			return pipelineWriteContext.getTxn();
		}
		public void flushUnderlyingBuffers() {
			pipelineWriteContext.flushUnderlyingBuffers();
		}

    }
