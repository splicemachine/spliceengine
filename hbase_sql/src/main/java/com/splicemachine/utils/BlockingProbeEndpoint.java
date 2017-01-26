/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.utils;

import java.io.IOException;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;

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
