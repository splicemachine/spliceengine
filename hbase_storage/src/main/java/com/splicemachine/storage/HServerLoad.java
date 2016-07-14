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
    public Set<PartitionLoad> getPartitionLoads(){
        Map<byte[], RegionLoad> regionsLoad=load.getRegionsLoad();
        Set<PartitionLoad> loads = new HashSet<>(regionsLoad.size(),0.9f);
        for(Map.Entry<byte[],RegionLoad> regionLoad:regionsLoad.entrySet()){
            String name = Bytes.toString(regionLoad.getKey());
            RegionLoad rl = regionLoad.getValue();
            PartitionLoad pl = new HPartitionLoad(name,rl.getStorefileSizeMB(),rl.getMemStoreSizeMB(),rl.getStorefileIndexSizeMB());
            loads.add(pl);
        }
        return loads;
    }
}
