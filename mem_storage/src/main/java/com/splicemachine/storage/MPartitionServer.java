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
}
