package com.splicemachine.hbase.batch;

import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.TableWriter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContext {
    void notRun(Mutation mutation);

    void sendUpstream(Mutation mutation);

    void failed(Mutation put, String exceptionMsg);

    void success(Mutation put);

    void result(Mutation put, MutationResult result);

    HRegion getRegion();

    HTableInterface getHTable(byte[] indexConglomBytes);

    CallBuffer<Mutation> getWriteBuffer(byte[] conglomBytes,TableWriter.FlushWatcher preFlushListener);

    RegionCoprocessorEnvironment getCoprocessorEnvironment();

    Map<Mutation,MutationResult> finish() throws IOException;

    boolean canRun(Mutation input);
}
