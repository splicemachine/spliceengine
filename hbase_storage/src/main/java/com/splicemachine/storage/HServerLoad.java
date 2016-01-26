package com.splicemachine.storage;

import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    @Override
    public Set<PartitionLoad> getPartitionLoads(){
        Map<byte[], RegionLoad> regionsLoad=load.getRegionsLoad();
        Set<PartitionLoad> loads = new HashSet<>(regionsLoad.size(),0.9f);
        for(Map.Entry<byte[],RegionLoad> regionLoad:regionsLoad.entrySet()){
            String name = Bytes.toString(regionLoad.getKey());
            RegionLoad rl = regionLoad.getValue();
            PartitionLoad pl = new HPartitionLoad(name,rl.getStorefileSizeMB(),rl.getMemStoreSizeMB(),rl.getStorefileIndexSizeMB());
            loads.add(pl);
        }
        return null;
    }
}
