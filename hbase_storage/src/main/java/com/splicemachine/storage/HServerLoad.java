/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
