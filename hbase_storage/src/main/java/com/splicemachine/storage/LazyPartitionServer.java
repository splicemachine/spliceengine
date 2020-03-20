/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
