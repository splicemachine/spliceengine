package com.splicemachine.hbase.batch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.hbase.writer.Writer;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContext {
    void notRun(KVPair mutation);

    void sendUpstream(KVPair mutation);
    
    void sendUpstream(List<KVPair> mutation);
    
    void failed(KVPair put, WriteResult mutationResult);

    void success(KVPair put);

    void result(KVPair put, WriteResult result);

    HRegion getRegion();

    HTableInterface getHTable(TableName indexConglomBytes);

    CallBuffer<KVPair> getWriteBuffer(TableName conglomBytes,
                                      WriteCoordinator.PreFlushHook preFlushListener,
                                      Writer.WriteConfiguration writeConfiguration,
                                      int expectedSize) throws Exception;

    RegionCoprocessorEnvironment getCoprocessorEnvironment();

    Map<KVPair,WriteResult> finish() throws IOException;

    boolean canRun(KVPair input);

    String getTransactionId();

		long getTransactionTimestamp();
}
