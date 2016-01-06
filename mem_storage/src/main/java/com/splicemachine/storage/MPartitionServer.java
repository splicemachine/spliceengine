package com.splicemachine.storage;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPartitionServer implements PartitionServer{
    @Override
    public int compareTo(PartitionServer o){
        return 0;  //all servers are the same in the in-memory version
    }

    @Override
    public String getHostname(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public String getHostAndPort(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public int getPort(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public PartitionServerLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public long getStartupTimestamp(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;
        else if(!(o instanceof PartitionServer)) return false;
        else return compareTo((PartitionServer)o)==0;
    }

    @Override
    public int hashCode(){
        return 1;
    }
}
