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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class LazyPartitionServer implements PartitionServer{
    private final HConnection connection;
    private final TableName tableName;
    private final HRegionInfo regionInfo;

    public LazyPartitionServer(HConnection connection, HRegionInfo regionInfo,TableName tableName){
        this.connection=connection;
        this.tableName = tableName;
        this.regionInfo=regionInfo;
    }

    @Override
    public int compareTo(PartitionServer o){
        return 0;
    }

    @Override
    public int hashCode(){
        return regionInfo.hashCode(); //actually should be the region server itself
    }

    @Override
    public boolean equals(Object obj){
        if(obj==this) return true;
        else if(!(obj instanceof PartitionServer)) return false;
        else return true;
    }
}

