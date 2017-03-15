/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.context;

import java.io.IOException;
import java.util.Map;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;

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

    @Override
    public Map<KVPair, WriteResult> currentResults(){
        return pipelineWriteContext.currentResults();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // WriteContext methods implemented by delegating to pipelineWriteContext
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
    public void result(byte[] rowKey, WriteResult result) {
        pipelineWriteContext.result(rowKey, result);
    }

    @Override
    public Partition remotePartition(byte[] indexConglomBytes) throws IOException{
        return pipelineWriteContext.remotePartition(indexConglomBytes);
    }

    @Override
    public CallBuffer<KVPair> getSharedWriteBuffer(byte[] conglomBytes,
                                                   ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                   int maxSize, boolean useAsyncWriteBuffers, TxnView txn) throws Exception {
        return pipelineWriteContext.getSharedWriteBuffer(conglomBytes, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn);
    }

    @Override
    public ServerControl getCoprocessorEnvironment() {
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
    public Partition getRegion() {
        return pipelineWriteContext.getRegion();
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

    @Override
    public String toString() {
        return "WriteNode { pipelineWriteContext=" + pipelineWriteContext.toString() + " }";
    }

    @Override
    public boolean skipIndexWrites() {
        return pipelineWriteContext.skipIndexWrites();
    }

    @Override
    public boolean skipConflictDetection() {
        return pipelineWriteContext.skipConflictDetection();
    }

    @Override
    public TransactionalRegion txnRegion(){
        return pipelineWriteContext.txnRegion();
    }

    @Override
    public PipelineExceptionFactory exceptionFactory(){
        return pipelineWriteContext.exceptionFactory();
    }
}
