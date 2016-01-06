package com.splicemachine.storage;

import org.apache.hadoop.hbase.ServerLoad;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HServerLoad implements PartitionServerLoad{
    private ServerLoad load;

    public HServerLoad(ServerLoad load){
        this.load=load;
    }

    @Override
    public long totalRequestCount(){
        return load.getTotalNumberOfRequests();
    }
}
