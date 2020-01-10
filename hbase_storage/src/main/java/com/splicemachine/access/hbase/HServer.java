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

package com.splicemachine.access.hbase;

import com.splicemachine.storage.HServerLoad;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.PartitionServerLoad;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HServer implements PartitionServer{
    private final ServerName serverName;
    private final ClusterStatus clusterStatus;

    public HServer(ServerName serverName, ClusterStatus clusterStatus) {
        this.serverName = serverName;
        this.clusterStatus = clusterStatus;
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
        ServerLoad load = clusterStatus.getLoad(serverName);
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
