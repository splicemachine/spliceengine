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

package com.splicemachine.pipeline.client;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RegionCoprocessorRpcChannel;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class Hbase10RpcChannelFactory implements RpcChannelFactory{
    private SConfiguration config;

    @Override
    public CoprocessorRpcChannel newChannel(TableName tableName,byte[] regionKey) throws IOException{
        Connection conn=HBaseConnectionFactory.getInstance(config).getNoRetryConnection();
        return new RegionCoprocessorRpcChannel((ClusterConnection) conn,tableName,regionKey);
    }

    @Override
    public void configure(SConfiguration config){
        this.config = config;
    }
}
