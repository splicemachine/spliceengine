/*
 * Copyright 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.data.hbase.coprocessor.SpliceRSRpcServices;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class GetLSNTask implements Callable<Void> {
    private ConcurrentHashMap<String, Long> map;
    private ServerName serverName;
    private String walGroupId;

    public GetLSNTask(ConcurrentHashMap<String, Long> map, ServerName serverName){
        this.map = map;
        this.serverName = serverName;
    }

    public GetLSNTask(ConcurrentHashMap<String, Long> map, ServerName serverName, String walGroupId){
        this.map = map;
        this.serverName = serverName;
        this.walGroupId = walGroupId;
    }

    @Override
    public Void call() throws Exception{
        SConfiguration configuration = HConfiguration.getConfiguration();
        Connection conn = HBaseConnectionFactory.getInstance(configuration).getConnection();
        Admin admin = conn.getAdmin();
        CoprocessorRpcChannel channel = admin.coprocessorService(serverName);
        SpliceRSRpcServices.BlockingInterface service = SpliceRSRpcServices.newBlockingStub(channel);
        SpliceMessage.GetRegionServerLSNRequest.Builder builder = SpliceMessage.GetRegionServerLSNRequest.newBuilder();
        if (walGroupId != null) {
            builder.setWalGroupId(walGroupId);
        }
        SpliceMessage.GetRegionServerLSNResponse response = service.getRegionServerLSN(null, builder.build());
        List<SpliceMessage.GetRegionServerLSNResponse.Result> resultList = response.getResultList();
        for (SpliceMessage.GetRegionServerLSNResponse.Result result : resultList) {
            map.put(result.getRegionName(), result.getLsn());
        }
        return null;
    }
}
