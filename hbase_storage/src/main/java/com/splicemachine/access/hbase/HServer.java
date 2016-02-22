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
}
