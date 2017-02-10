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
