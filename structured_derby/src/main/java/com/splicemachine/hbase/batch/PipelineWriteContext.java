package com.splicemachine.hbase.batch;

import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.ThrowIfDisconnected;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.hbase.writer.Writer;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class PipelineWriteContext implements WriteContext{
    private final Map<KVPair,WriteResult> resultsMap;
    private final TransactionalRegion rce;
    private final RegionCoprocessorEnvironment env;

    private final Map<byte[],HTableInterface> tableCache = Maps.newHashMapWithExpectedSize(0);
    private static final Logger LOG = Logger.getLogger(PipelineWriteContext.class);
		private long timestamp;

    private class WriteNode implements WriteContext{
        private WriteHandler handler;
        private WriteNode next;

        public WriteNode(WriteHandler handler){ this.handler = handler; }

        @Override
        public void notRun(KVPair mutation) {
            PipelineWriteContext.this.notRun(mutation);
        }

        @Override
        public void sendUpstream(KVPair mutation) {
            if(next!=null)
                next.handler.next(mutation,next);
        }

        @Override
        public void failed(KVPair put, WriteResult mutationResult) {
            PipelineWriteContext.this.failed(put,mutationResult);
        }

        @Override
        public void success(KVPair put) {
            PipelineWriteContext.this.success(put);
        }

        @Override
        public void result(KVPair put, WriteResult result) {
            PipelineWriteContext.this.result(put,result);
        }

        @Override
        public HRegion getRegion() {
            return getCoprocessorEnvironment().getRegion();
        }

        @Override
        public HTableInterface getHTable(byte[] indexConglomBytes) {
            return PipelineWriteContext.this.getHTable(indexConglomBytes);
        }

        @Override
        public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                                 WriteCoordinator.PreFlushHook preFlushListener,
                                                 Writer.WriteConfiguration writeConfiguration,
                                                 int maxEntries) throws Exception {
            return PipelineWriteContext.this.getWriteBuffer(conglomBytes,preFlushListener, writeConfiguration,maxEntries);
        }

        @Override
        public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
            return PipelineWriteContext.this.getCoprocessorEnvironment();
        }

        @Override
        public Map<KVPair,WriteResult> finish() throws IOException {
            handler.finishWrites(this);
            return null; //ignored
        }

        @Override
        public boolean canRun(KVPair input) {
            return PipelineWriteContext.this.canRun(input);
        }

				@Override
				public long getTransactionTimestamp() {
						return PipelineWriteContext.this.getTransactionTimestamp();
				}

				@Override
				public Txn getTxn() {
						return PipelineWriteContext.this.getTxn();
				}

				@Override
		public void sendUpstream(List<KVPair> mutation) {
			// XXX JLEACH TODO
			throw new RuntimeException("Not Supported");
		}
    }

    private WriteNode head;
    private WriteNode tail;
    private final boolean keepState;
    private final boolean useAsyncWriteBuffers;
    private final Txn txn;

    public PipelineWriteContext(Txn txn, TransactionalRegion rce, RegionCoprocessorEnvironment env) {
        this(txn,rce,env,true,false);
    }

    public PipelineWriteContext(Txn txn,TransactionalRegion rce,RegionCoprocessorEnvironment env,boolean keepState,boolean useAsyncWriteBuffers) {
        this.rce = rce;
        this.resultsMap = Maps.newIdentityHashMap();
        this.keepState = keepState;
        this.useAsyncWriteBuffers= useAsyncWriteBuffers;
				this.txn = txn;

        head = tail =new WriteNode(null);
        this.env = env;
    }

    public void addLast(WriteHandler handler){
        if(tail!=null){
            tail.next = new WriteNode(handler);
            tail = tail.next;
        }
    }


    @Override
    public void notRun(KVPair mutation) {
        if(keepState)
            resultsMap.put(mutation,WriteResult.notRun());
    }

    @Override
    public void sendUpstream(KVPair mutation) {
        head.sendUpstream(mutation);
    }

    @Override
    public void failed(KVPair put, WriteResult mutationResult) {
        if(keepState)
            resultsMap.put(put, mutationResult);
        else
            throw new RuntimeException(Exceptions.fromString(mutationResult));
    }

    @Override
    public void success(KVPair put) {
        if(keepState)
            resultsMap.put(put,WriteResult.success());
    }

    @Override
    public void result(KVPair put, WriteResult result) {
        if(keepState)
            resultsMap.put(put,result);
    }

    @Override
    public HRegion getRegion() {
        return getCoprocessorEnvironment().getRegion();
    }

    @Override
    public HTableInterface getHTable(byte[] indexConglomBytes) {
        HTableInterface table = tableCache.get(indexConglomBytes);
        if(table==null){
            try {
                table = getCoprocessorEnvironment().getTable(indexConglomBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            tableCache.put(indexConglomBytes,table);
        }
        return table;
    }

    @Override
    public CallBuffer<KVPair> getWriteBuffer(byte[] conglomBytes,
                                             WriteCoordinator.PreFlushHook preFlushListener,
                                             Writer.WriteConfiguration writeConfiguration, int maxSize) throws Exception {
        if(useAsyncWriteBuffers)
            return SpliceDriver.driver().getTableWriter().writeBuffer(conglomBytes,txn, preFlushListener, writeConfiguration);
        return SpliceDriver.driver().getTableWriter().synchronousWriteBuffer(conglomBytes,txn,preFlushListener, writeConfiguration,maxSize);
    }

    @Override
    public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
        return env;
    }
    
	
	@Override
    public Map<KVPair,WriteResult> finish() throws IOException {
        RpcCallContext currentCall = HBaseServer.getCurrentCall();
        if(currentCall!=null)
        	ThrowIfDisconnected.getThrowIfDisconnected().invoke(currentCall, rce.getRegionName());
        
        try{
            WriteNode next = head.next;
            while(next!=null){
                next.finish();
                next = next.next;
            }
        }finally{
            //clean up any outstanding table resources
            for(HTableInterface table:tableCache.values()){
                try{
                    table.close();
                }catch(Exception e){
                    //don't need to interrupt the finishing of this batch just because
                    //we got an error. Log it and move on
                    LOG.warn("Unable to clone table",e);
                }
            }
        }
        return resultsMap;
    }

    @Override
    public boolean canRun(KVPair input) {
        WriteResult result = resultsMap.get(input);
        return result == null || result.getCode() == WriteResult.Code.SUCCESS;
    }

		@Override
		public long getTransactionTimestamp() {
				return txn.getBeginTimestamp();
		}

		@Override
		public Txn getTxn() {
				return txn;
		}

		@Override
	public void sendUpstream(List<KVPair> mutation) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
	}
}
