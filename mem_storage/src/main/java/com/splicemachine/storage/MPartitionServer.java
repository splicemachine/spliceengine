package com.splicemachine.storage;

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
    public boolean equals(Object o){
        if(o==this) return true;
        else if(!(o instanceof MPartitionServer)) return false;
        else return true;
    }

    @Override
    public int hashCode(){
        return 1;
    }
}
