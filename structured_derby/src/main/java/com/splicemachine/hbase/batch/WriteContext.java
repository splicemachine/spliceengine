package com.splicemachine.hbase.batch;

import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.writer.TableWriter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContext {
    void notRun(Mutation mutation);

    void sendUpstream(Mutation mutation);

    void failed(Mutation put, MutationResult mutationResult);

    void success(Mutation put);

    void result(Mutation put, MutationResult result);

    HRegion getRegion();

    HTableInterface getHTable(byte[] indexConglomBytes);

    CallBuffer<Mutation> getWriteBuffer(byte[] conglomBytes,TableWriter.FlushWatcher preFlushListener) throws Exception;

    RegionCoprocessorEnvironment getCoprocessorEnvironment();

    Map<Mutation,MutationResult> finish() throws IOException;

    boolean canRun(Mutation input);
}
