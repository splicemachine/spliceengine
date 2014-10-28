package com.splicemachine.pipeline.writecontext;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.ThrowIfDisconnected;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.SharedCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class PipelineWriteContext implements WriteContext, Comparable{
    private static final Logger LOG = Logger.getLogger(PipelineWriteContext.class);
	private final Map<KVPair,WriteResult> resultsMap;
    private final TransactionalRegion rce;
    private final Map<byte[],HTableInterface> tableCache = Maps.newHashMapWithExpectedSize(0);
	private long timestamp;
    private WriteNode head;
    private WriteNode tail;
    private final boolean keepState;
    private final boolean useAsyncWriteBuffers;
    private final TxnView txn;
    private SharedCallBuffer indexSharedCallBuffer;
    private static final AtomicInteger idGen = new AtomicInteger(0);
    private final int id = idGen.incrementAndGet();
    private RegionCoprocessorEnvironment env;
    
    public PipelineWriteContext(SharedCallBuffer indexSharedCallBuffer,TxnView txn,TransactionalRegion rce,  RegionCoprocessorEnvironment env) {
        this(indexSharedCallBuffer,txn,rce,env,true,true);
    }

    public PipelineWriteContext(SharedCallBuffer indexSharedCallBuffer,TxnView txn,TransactionalRegion rce, RegionCoprocessorEnvironment env,boolean keepState,boolean useAsyncWriteBuffers) {
        this.indexSharedCallBuffer = indexSharedCallBuffer;
        this.env = env;
    	this.rce = rce;
        this.resultsMap = Maps.newIdentityHashMap();
        this.keepState = keepState;
        this.useAsyncWriteBuffers= useAsyncWriteBuffers;
		this.txn = txn;
        head = tail =new WriteNode(null,this);
        if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "instance");
    }

    public void addLast(WriteHandler handler){    		
        if(tail!=null){
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "addLast %s", handler);
            tail.next = new WriteNode(handler,this);
            tail = tail.next;
        }
    }


    @Override
    public void notRun(KVPair mutation) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "notRun %s", mutation);
        if(keepState)
            resultsMap.put(mutation,WriteResult.notRun());
    }

    @Override
    public void sendUpstream(KVPair mutation) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "sendUpstream %s", mutation);
        head.sendUpstream(mutation);
    }

    @Override
    public void failed(KVPair put, WriteResult mutationResult) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "failed put=%s, mutationResult=%s", put,mutationResult);
        if(keepState)
            resultsMap.put(put, mutationResult);
        else
            throw new RuntimeException(Exceptions.fromString(mutationResult));
    }

    @Override
    public void success(KVPair put) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "success put=%s", put);
        if(keepState)
            resultsMap.put(put,WriteResult.success());
    }

    @Override
    public void result(KVPair put, WriteResult result) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "result put=%s, result=%s", put, result);
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
	public CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
			ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
			int maxSize, boolean useAsyncWriteBuffers, TxnView txn)
			throws Exception {
    	assert indexSharedCallBuffer != null;
    	return indexSharedCallBuffer.getWriteBuffer(conglomBytes, this, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
	}
        
	
	@Override
    public void flush() throws IOException {
    	if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "flush");
        RpcCallContext currentCall = HBaseServer.getCurrentCall();
        if(currentCall!=null)
        	ThrowIfDisconnected.getThrowIfDisconnected().invoke(currentCall, rce.getRegionName());
        
        try{
            WriteNode next = head.next;
            while(next!=null){
                next.flush();
                next = next.next;
            }
            next = head.next;
            while(next!=null){
                next.close();
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
        if (LOG.isTraceEnabled())
        	SpliceLogUtils.trace(LOG,"flush=%s",resultsMap);
    }

    @Override
    public boolean canRun(KVPair input) {
        WriteResult result = resultsMap.get(input);
        return result == null || result.getCode() == Code.SUCCESS;
    }

		@Override
		public long getTransactionTimestamp() {
				return txn.getBeginTimestamp();
		}

		@Override
		public TxnView getTxn() {
				return txn;
		}

		@Override
	public void sendUpstream(List<KVPair> mutation) {
		throw new RuntimeException("Not Supported");
	}

		@Override
		public Map<KVPair, WriteResult> close() throws IOException {
	    	if (LOG.isDebugEnabled())
	    		SpliceLogUtils.debug(LOG, "close");
	        return resultsMap;
		}

		@Override
		public void flushUnderlyingBuffers() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public String toString() {
			return "PipelineWriteContext { region=" + rce.getRegionName() + " }";
		}

		@Override
		public int compareTo(Object object) {
			return this.id - ((PipelineWriteContext) object).id;
			// TODO Auto-generated method stub
		}

		@Override
		public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
			return env;
		}
		
		
		
		
}
