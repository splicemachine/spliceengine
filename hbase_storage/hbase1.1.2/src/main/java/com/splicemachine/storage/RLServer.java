package com.splicemachine.storage;

import org.apache.hadoop.hbase.HRegionLocation;

import java.io.IOException;

/**
 * An implementation of PartitionServer which defers to an HRegionLocation instance. It could be wrong
 * if the region moves.
 *
 * @author Scott Fines
 *         Date: 3/4/16
 */
public class RLServer implements PartitionServer{
    private final HRegionLocation regionLocation;

    public RLServer(HRegionLocation regionLocation){
        this.regionLocation=regionLocation;
    }

    @Override
    public String getHostname(){
        return regionLocation.getHostname();
    }

    @Override
    public String getHostAndPort(){
        return regionLocation.getHostnamePort();
    }

    @Override
    public int getPort(){
        return regionLocation.getPort();
    }

    @Override
    public PartitionServerLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public long getStartupTimestamp(){
        return regionLocation.getServerName().getStartcode();
    }

    @Override
    public int compareTo(PartitionServer o){
        assert o instanceof RLServer: "Cannot compare to non RegionLocationServer";
        return regionLocation.compareTo(((RLServer)o).regionLocation);
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof RLServer)) return false;

        RLServer rlServer=(RLServer)o;

        return regionLocation.equals(rlServer.regionLocation);
    }

    @Override
    public int hashCode(){
        return regionLocation.hashCode();
    }
}
