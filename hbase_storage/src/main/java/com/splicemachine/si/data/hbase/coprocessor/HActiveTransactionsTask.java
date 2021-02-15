/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.data.hbase.coprocessor;

import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.util.List;

public class HActiveTransactionsTask implements com.splicemachine.access.api.ActiveTransactionsTask {
    private ServerName serverName;
    private long minTxnId;
    private long maxTxnId;
    private byte[] destinationTable;

    public HActiveTransactionsTask(ServerName serverName, long minTxnId, long maxTxnId, byte[] destinationTable){
        this.serverName = serverName;
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.destinationTable = destinationTable;
    }

    @Override
    public List<Long> call() throws Exception {
        SConfiguration configuration = HConfiguration.getConfiguration();
        Connection conn = HBaseConnectionFactory.getInstance(configuration).getConnection();
        Admin admin = conn.getAdmin();
        CoprocessorRpcChannel channel = admin.coprocessorService(serverName);
        SpliceRSRpcServices.BlockingInterface service = SpliceRSRpcServices.newBlockingStub(channel);
        SpliceMessage.SpliceActiveTransactionsRequest request = SpliceMessage.SpliceActiveTransactionsRequest.newBuilder()
                .setMinTxnId(minTxnId)
                .setMaxTxnId(maxTxnId)
                .setDestinationTable(ZeroCopyLiteralByteString.wrap(destinationTable)).build();
        SpliceMessage.SpliceActiveTransactionsResponse response = service.getActiveTransactions(null, request);
        return response.getActiveTransactionsList();
    }
}
