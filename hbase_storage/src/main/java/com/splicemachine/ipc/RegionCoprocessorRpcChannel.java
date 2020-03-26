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

package com.splicemachine.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jyuan on 4/5/19.
 */
class RegionCoprocessorRpcChannel extends SyncCoprocessorRpcChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RegionCoprocessorRpcChannel.class);
    private final TableName table;
    private final byte[] row;
    private final ClusterConnection conn;
    private byte[] lastRegion;
    private final int operationTimeout;
    private final RpcRetryingCallerFactory rpcCallerFactory;

    RegionCoprocessorRpcChannel(ClusterConnection conn, TableName table, byte[] row) {
        this.table = table;
        this.row = row;
        this.conn = conn;
        this.operationTimeout = conn.getConnectionConfiguration().getOperationTimeout();
        this.rpcCallerFactory = conn.getRpcRetryingCallerFactory();
    }

    protected Message callExecService(RpcController controller, final Descriptors.MethodDescriptor method, final Message request, Message responsePrototype) throws IOException {
        if(LOG.isTraceEnabled()) {
            LOG.trace("Call: " + method.getName() + ", " + request.toString());
        }

        if(this.row == null) {
            throw new NullPointerException("Can\'t be null!");
        } else {
            int priority = HConstants.PRIORITY_UNSET;
            if (controller instanceof SpliceRpcController) {
                priority = ((SpliceRpcController)controller).getPriority();
            }
            ClientServiceCallable callable = new ClientServiceCallable(this.conn, this.table, this.row, this.conn.getRpcControllerFactory().newController(), priority) {
                protected ClientProtos.CoprocessorServiceResponse rpcCall() throws Exception {
                    byte[] regionName = this.getLocation().getRegionInfo().getRegionName();
                    ClientProtos.CoprocessorServiceRequest csr = CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request, RegionCoprocessorRpcChannel.this.row, regionName);
                    return ((ClientProtos.ClientService.BlockingInterface)this.getStub()).execService(this.getRpcController(), csr);
                }
            };
            ClientProtos.CoprocessorServiceResponse result = (ClientProtos.CoprocessorServiceResponse)this.rpcCallerFactory.newCaller().callWithRetries(callable, this.operationTimeout);
            this.lastRegion = result.getRegion().getValue().toByteArray();
            return CoprocessorRpcUtils.getResponse(result, responsePrototype);
        }
    }

    public byte[] getLastRegion() {
        return this.lastRegion;
    }
}
