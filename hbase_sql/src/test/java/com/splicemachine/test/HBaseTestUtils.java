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

package com.splicemachine.test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

/**
 * Test utilities requiring HBase
 */
public class HBaseTestUtils {

    enum CallType {
        POST_COMPACT,
        PRE_COMPACT,
        POST_FLUSH,
        PRE_FLUSH,
        POST_SPLIT,
        PRE_SPLIT,
    }

    private static boolean setBlock(final boolean onOff, CallType type) throws Throwable {
        org.apache.hadoop.hbase.client.Connection hbaseConnection = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection();
        Admin admin = hbaseConnection.getAdmin();
        ServerRpcController controller = new ServerRpcController();
        SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
        final AtomicBoolean success = new AtomicBoolean(true);
        Collection<ServerName> servers = admin.getClusterStatus().getServers();
        final CountDownLatch latch = new CountDownLatch(servers.size());
        for (ServerName server : servers) {
            CoprocessorRpcChannel channel = admin.coprocessorService(server);
            SpliceMessage.BlockingProbeEndpoint.Stub service = SpliceMessage.BlockingProbeEndpoint.newStub(channel);
            RpcCallback<SpliceMessage.BlockingProbeResponse> callback = new RpcCallback<SpliceMessage.BlockingProbeResponse>() {
                @Override
                public void run(SpliceMessage.BlockingProbeResponse response) {
                    if (response.getDidBlock() != onOff) {
                        success.set(false);
                    }
                    latch.countDown();
                }
            };
            switch (type) {
                case POST_COMPACT: service.blockPostCompact(controller, message, callback); break;
                case PRE_COMPACT: service.blockPreCompact(controller, message, callback); break;
                case POST_FLUSH: service.blockPostFlush(controller, message, callback); break;
                case PRE_FLUSH: service.blockPreFlush(controller, message, callback); break;
                case POST_SPLIT: service.blockPostSplit(controller, message, callback); break;
                case PRE_SPLIT: service.blockPreSplit(controller, message, callback); break;
            }
        }
        if (!latch.await(10000, TimeUnit.SECONDS)){
            return false;
        }
        return success.get();
    }

    public static boolean setBlockPreFlush(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.PRE_FLUSH);
    }

    public static boolean setBlockPostFlush(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.POST_FLUSH);
    }

    public static boolean setBlockPreCompact(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.PRE_COMPACT);
    }

    public static boolean setBlockPostCompact(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.POST_COMPACT);
    }

    public static boolean setBlockPreSplit(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.PRE_SPLIT);
    }

    public static boolean setBlockPostSplit(final boolean onOff) throws Throwable {
        return setBlock(onOff, CallType.POST_SPLIT);
    }

}
