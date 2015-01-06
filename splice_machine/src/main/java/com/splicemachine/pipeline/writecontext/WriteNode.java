package com.splicemachine.pipeline.writecontext;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Map;

public class WriteNode implements WriteContext {

    private WriteHandler handler;
    private WriteNode next;
    private PipelineWriteContext pipelineWriteContext;

    public WriteNode(WriteHandler handler, PipelineWriteContext pipelineWriteContext) {
        this.handler = handler;
        this.pipelineWriteContext = pipelineWriteContext;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // WriteContext implemented using our handler or next node's handler
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public void sendUpstream(KVPair mutation) {
        if (next != null) {
            next.handler.next(mutation, next);
        }
    }

    @Override
    public void flush() throws IOException {
        handler.flush(this);
    }

    @Override
    public Map<KVPair, WriteResult> close() throws IOException {
        handler.close(this);
        return null; //ignored
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // WriteContext implemented by delegating to pipelineWriteContext
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public void notRun(KVPair mutation) {
        pipelineWriteContext.notRun(mutation);
    }

    @Override
    public void failed(KVPair put, WriteResult mutationResult) {
        pipelineWriteContext.failed(put, mutationResult);
    }

    @Override
    public void success(KVPair put) {
        pipelineWriteContext.success(put);
    }

    @Override
    public void result(KVPair put, WriteResult result) {
        pipelineWriteContext.result(put, result);
    }

    @Override
    public HTableInterface getHTable(byte[] indexConglomBytes) {
        return pipelineWriteContext.getHTable(indexConglomBytes);
    }

    @Override
    public CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
                                                   ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                   int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception {
        return pipelineWriteContext.getSharedWriteBuffer(conglomBytes, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
    }

    @Override
    public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
        return pipelineWriteContext.getCoprocessorEnvironment();
    }

    @Override
    public boolean canRun(KVPair input) {
        return pipelineWriteContext.canRun(input);
    }

    @Override
    public TxnView getTxn() {
        return pipelineWriteContext.getTxn();
    }

    @Override
    public HRegion getRegion() {
        return getCoprocessorEnvironment().getRegion();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // getter/setter
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public WriteNode getNext() {
        return next;
    }

    public void setNext(WriteNode next) {
        this.next = next;
    }
}
