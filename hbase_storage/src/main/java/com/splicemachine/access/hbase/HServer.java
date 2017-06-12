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

package com.splicemachine.access.hbase;

import com.splicemachine.storage.HServerLoad;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.PartitionServerLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HServer implements PartitionServer{
    private final ServerName serverName;
    private final Admin admin;

    public HServer(ServerName serverName,Admin admin){
        this.serverName=serverName;
        this.admin=admin;
    }

    @Override
    public String getHostname(){
        return serverName.getHostname();
    }

    @Override
    public String getHostAndPort(){
        return serverName.getHostAndPort();
    }

    @Override
    public int getPort(){
        return serverName.getPort();
    }

    @Override
    public PartitionServerLoad getLoad() throws IOException{
        ServerLoad load=admin.getClusterStatus().getLoad(serverName);
        return new HServerLoad(load);
    }

    @Override
    public long getStartupTimestamp(){
        return serverName.getStartcode();
    }

    @Override
    public int compareTo(PartitionServer o){
        //TODO -sf- compare only on hostnameport etc.
        return serverName.compareTo(((HServer)o).serverName);
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof HServer)) return false;

        HServer hServer=(HServer)o;

        return serverName.equals(hServer.serverName);
    }

    @Override
    public int hashCode(){
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "HServer{" +
                "serverName=" + serverName +
                '}';
    }
}
