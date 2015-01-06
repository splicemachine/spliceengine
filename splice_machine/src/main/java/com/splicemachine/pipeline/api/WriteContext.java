package com.splicemachine.pipeline.api;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for the write context
 *
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public interface WriteContext {

    /**
     * Do not run the following KVPair
     */
    void notRun(KVPair mutation);

    /**
     * Send KVPair upstream to be processes by possible handlers.
     */
    void sendUpstream(KVPair mutation);

    /**
     * Fail the following KVPair and put them into the WriteResult
     */
    void failed(KVPair put, WriteResult mutationResult);

    /**
     * Mark KVPair as successful
     */
    void success(KVPair put);

    void result(KVPair put, WriteResult result);

    /**
     * Retrieve the region from the context
     */
    HRegion getRegion();

    /**
     * Retrieve the HTableInterface based on the index bytes[] name
     */
    HTableInterface getHTable(byte[] indexConglomBytes);

    /**
     * Retrieve the sharedWriteBuffer for the index upsert handler
     */
    CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
                                            ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                            int maxSize,
                                            boolean useAsyncWriteBuffers,
                                            TxnView txn) throws Exception;

    /**
     * Retrieve the coprocessor environment
     */
    RegionCoprocessorEnvironment getCoprocessorEnvironment();

    /**
     * Flush
     */
    void flush() throws IOException;

    /**
     * Close
     */
    Map<KVPair, WriteResult> close() throws IOException;

    /**
     * CanRun
     */
    boolean canRun(KVPair input);

    /**
     * Retrieve Transaction
     */
    TxnView getTxn();

}