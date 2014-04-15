package com.splicemachine.hbase.batch;

import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.RegionWriteHandler.m_Invoke;
import com.splicemachine.hbase.batch.RegionWriteHandler.m_Invoker;
import com.splicemachine.hbase.writer.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.HRegionUtil.KeyExists;
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
    private final RegionCoprocessorEnvironment rce;

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
        public String getTransactionId() {
            return PipelineWriteContext.this.getTransactionId();
        }

				@Override
				public long getTransactionTimestamp() {
						return PipelineWriteContext.this.getTransactionTimestamp();
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
    private final String txnId;

    public PipelineWriteContext(String txnId,RegionCoprocessorEnvironment rce) {
        this(txnId,rce,true,false);
    }

    public PipelineWriteContext(String txnId,RegionCoprocessorEnvironment rce,boolean keepState,boolean useAsyncWriteBuffers) {
        this.rce = rce;
        this.resultsMap = Maps.newIdentityHashMap();
        this.keepState = keepState;
        this.useAsyncWriteBuffers= useAsyncWriteBuffers;
        this.txnId = txnId;

        head = tail =new WriteNode(null);
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
            return SpliceDriver.driver().getTableWriter().writeBuffer(conglomBytes,txnId, preFlushListener, writeConfiguration);
        return SpliceDriver.driver().getTableWriter().synchronousWriteBuffer(conglomBytes,txnId,preFlushListener, writeConfiguration,maxSize);
    }

    @Override
    public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
        return rce;
    }
    
    private static boolean is43 = true;
    private static Method m=null;
    private static m_Invoke m_invoker;
    public interface m_Invoker {
    	public void m_invoke(RpcCallContext target,String message) throws IOException;
    }

    private void OnFirstCall(RpcCallContext target,String message) {
    	if(m==null){
		     try {
		    	 m = target.getClass().getMethod("throwExceptionIfCallerDisconnected", new Class[] {});
		     } catch (NoSuchMethodException e){
		    	 is43=false;
		    	 try {
		    		 m = target.getClass().getMethod("throwExceptionIfCallerDisconnected", new Class[] {String.class});
		    	 } catch (NoSuchMethodException e1){
		    		 e1.printStackTrace();
		    	 }catch(SecurityException e1){
		    		 e1.printStackTrace();
		    	 }
		     }
    	}
     	m_invoker = new m_Invoke();
    }

    public static class m_Invoke implements m_Invoker{
    	public static m_Invoke  iinvoke = null;
    	private  m_Invoke getInvoker(RpcCallContext target,String message){
    	if(is43){
    		return new m_Invoke(){
    			@Override
    				public void m_invoke(RpcCallContext target,String message) throws IOException {
    					try {
    						m.invoke(target,  new Object[] {});
    					} catch (IllegalAccessException e){
    						e.printStackTrace();
    					}catch(IllegalArgumentException e){
    						e.printStackTrace();
    					}catch(InvocationTargetException e) {
    						e.printStackTrace();
    					}
    			}
    		};
    	}else{
    		return new m_Invoke(){
    			@Override
    			public void m_invoke(RpcCallContext target,String message) throws IOException {
    				try {
    					m.invoke(target,   new Object[] {message});
    				} catch (IllegalAccessException e){
    					e.printStackTrace();
    				}catch(IllegalArgumentException e){
    					e.printStackTrace();
    				}catch(InvocationTargetException e) {
    					e.printStackTrace();
    				}
    			}
    		};
    	}
    	}

		@Override
		public void m_invoke(RpcCallContext target, String message)
				throws IOException {
			iinvoke = getInvoker(target,message);
		}
    }


	
	@Override
    public Map<KVPair,WriteResult> finish() throws IOException {
        RpcCallContext currentCall = HBaseServer.getCurrentCall();
        if(currentCall!=null){
        	if(m_invoker==null){
        		OnFirstCall(currentCall,rce.getRegion().getRegionNameAsString());
        	}
    		m_invoker.getInvoker(currentCall,rce.getRegion().getRegionNameAsString());
        }
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
    public String getTransactionId() {
        return txnId;
    }

		@Override
		public long getTransactionTimestamp() {
				if(timestamp<=0){
						/*
						 * It's cheaper to try parsing (succeeding 99.9999999% of the time) and
						 * fail when NA_TRANSACTION_ID is passed than it is to do the comparison every time.
						 */
						try{
								timestamp = Long.parseLong(txnId);
						}catch(NumberFormatException nfe){
							if(SpliceConstants.NA_TRANSACTION_ID.equals(txnId)){
									timestamp=0;
							}
						}
				}

				return timestamp;
		}

		@Override
	public void sendUpstream(List<KVPair> mutation) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
	}
}
