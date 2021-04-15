/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.context;

import java.io.IOException;
import java.util.Map;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.carrotsearch.hppc.ObjectObjectHashMap;
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
                                                   ObjectObjectHashMap<KVPair, KVPair> indexToMainMutationMap,
                                                   int maxSize, boolean useAsyncWriteBuffers, TxnView txn, byte[] token) throws Exception {
        return pipelineWriteContext.getSharedWriteBuffer(conglomBytes, indexToMainMutationMap, maxSize, useAsyncWriteBuffers, txn, token);
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
    public byte[] getToken() {
        return pipelineWriteContext.getToken();
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
    public boolean skipWAL() {
        return pipelineWriteContext.skipWAL();
    }

    @Override
    public boolean rollforward() {
        return pipelineWriteContext.rollforward();
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
