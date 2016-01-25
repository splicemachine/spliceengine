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
    public int numPartitions(){
        return load.getNumberOfRegions();
    }

    @Override
    public long totalWriteRequests(){
        return load.getWriteRequestsCount();
    }

    @Override
    public long totalReadRequests(){
        return load.getReadRequestsCount();
    }

    @Override
    public long totalRequests(){
        return load.getTotalNumberOfRequests();
    }

    @Override
    public int compactionQueueLength(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public int flushQueueLength(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
