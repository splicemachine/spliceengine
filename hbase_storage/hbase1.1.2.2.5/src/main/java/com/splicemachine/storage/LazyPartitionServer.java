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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class LazyPartitionServer implements PartitionServer{
    private final Connection connection;
    private final TableName tableName;
    private final HRegionInfo regionInfo;

    public LazyPartitionServer(Connection connection, HRegionInfo regionInfo,TableName tableName){
        this.connection=connection;
        this.tableName = tableName;
        this.regionInfo=regionInfo;
    }

    @Override
    public int compareTo(@Nullable PartitionServer o){
        //TODO -sf- implement
       return 0;
    }

    @Override
    public int hashCode(){
        return regionInfo.hashCode(); //actually should be the region server itself
    }

    @Override
    public String getHostname(){
        try(RegionLocator rl = connection.getRegionLocator(tableName)){
            HRegionLocation hrl =rl.getRegionLocation(regionInfo.getStartKey());
            return hrl.getHostname();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getHostAndPort(){
        try(RegionLocator rl = connection.getRegionLocator(tableName)){
            HRegionLocation hrl =rl.getRegionLocation(regionInfo.getStartKey());
            return hrl.getHostnamePort();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPort(){
        try(RegionLocator rl = connection.getRegionLocator(tableName)){
            HRegionLocation hrl =rl.getRegionLocation(regionInfo.getStartKey());
            return hrl.getPort();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitionServerLoad getLoad(){
        try(RegionLocator rl = connection.getRegionLocator(tableName)){
            HRegionLocation hrl =rl.getRegionLocation(regionInfo.getStartKey());
            try(Admin admin = connection.getAdmin()){
                ServerLoad load=admin.getClusterStatus().getLoad(hrl.getServerName());
                return new HServerLoad(load);
            }
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getStartupTimestamp(){
        try(RegionLocator rl = connection.getRegionLocator(tableName)){
            HRegionLocation hrl=rl.getRegionLocation(regionInfo.getStartKey());
            return hrl.getServerName().getStartcode();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object obj){
        if(obj==this) return true;
        else if(!(obj instanceof PartitionServer)) return false;
        return compareTo((PartitionServer)obj)==0;
    }
}
