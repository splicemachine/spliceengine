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

package com.splicemachine.utils;

import java.io.IOException;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;

/**
 * An HBase coprocessor used in testing to activate a {@link BlockingProbe} for a given administration stage
 * on all regions.
 */
public class BlockingProbeEndpoint extends SpliceMessage.BlockingProbeEndpoint implements SingletonCoprocessorService,Coprocessor {

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // no-op
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // no-op
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void blockPreCompact(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback<SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPreCompact(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPreCompact()).build());
    }

    @Override
    public void blockPostCompact(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback<SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPostCompact(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPostCompact()).build());
    }

    @Override
    public void blockPreSplit(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback<SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPreSplit(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPreSplit()).build());
    }

    @Override
    public void blockPostSplit(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback<SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPostSplit(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPostSplit()).build());
    }

    @Override
    public void blockPreFlush(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback<SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPreFlush(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPreFlush()).build());
    }

    @Override
    public void blockPostFlush(RpcController controller, SpliceMessage.BlockingProbeRequest request, RpcCallback
            <SpliceMessage.BlockingProbeResponse> done) {
        BlockingProbe.setBlockPostFlush(request.getDoBlock());
        done.run(SpliceMessage.BlockingProbeResponse.newBuilder().setDidBlock(BlockingProbe.isBlockPostFlush()).build());
    }
}
