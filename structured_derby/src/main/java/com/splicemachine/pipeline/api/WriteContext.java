package com.splicemachine.pipeline.api;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 
 * Interface for the write context 
 * 
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContext {
	/**
	 * Do not run the following KVPair
	 * 
	 * @param mutation
	 */
    void notRun(KVPair mutation);
    /**
     * Send KVPair upstream to be processes by possible handlers.
     * 
     */
    void sendUpstream(KVPair mutation);
    /**
     * 
     * Send List of KVPairs to be processed upstream by possible handlers.
     * 
     * @param mutation
     */
    void sendUpstream(List<KVPair> mutation);
    /**
     * Fail the following KVPair and put them into the WriteResult
     * 
     * @param put
     * @param mutationResult
     */
    void failed(KVPair put, WriteResult mutationResult);
    /**
     * Mark KVPair as successful
     * 
     * @param put
     */
    void success(KVPair put);

    void result(KVPair put, WriteResult result);
    /**
     * Retrieve the region from the context
     * 
     * @return
     */
    HRegion getRegion();
    /**
     * Retrieve the HTableInterface based on the index bytes[] name
     * 
     * @param indexConglomBytes
     * @return
     */
    HTableInterface getHTable(byte[] indexConglomBytes);
    /**
     * 
     * Retrieve the sharedWriteBuffer for the index upsert handler
     * 
     * @param conglomBytes
     * @param preFlushListener
     * @param writeConfiguration
     * @param expectedSize
     * @return
     * @throws Exception
     */
    CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
    		ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap,
    							int maxSize,
    							boolean useAsyncWriteBuffers,
    							TxnView txn) throws Exception;

      
    /**
     * 
     * Retrieve the coprocessor environment
     * 
     * @return
     */
    RegionCoprocessorEnvironment getCoprocessorEnvironment();
    /**
     * Flush
     * 
     * @return
     * @throws IOException
     */
    void flush() throws IOException;
    /**
     * Close
     * 
     * @return
     * @throws IOException
     */
    Map<KVPair,WriteResult> close() throws IOException;    
    /**
     * CanRun
     * 
     * @param input
     * @return
     */
    boolean canRun(KVPair input);
    /**
     * Retrieve Transacion
     * 
     * @return
     */
    TxnView getTxn();
    /**
     * Retrieve transation Timestamp
     * 
     * @return
     */
	long getTransactionTimestamp();
	
	
	void flushUnderlyingBuffers();
}
