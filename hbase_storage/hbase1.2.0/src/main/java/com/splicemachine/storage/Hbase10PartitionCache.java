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

import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class Hbase10PartitionCache implements PartitionInfoCache<TableName>{
    private SConfiguration config;
    private HBaseTableInfoFactory tableInfoFactory;
    private Cache<TableName, List<Partition>> partitionCache = CacheBuilder.newBuilder().maximumSize(100).build();

    //must be a no-args to support the PartitionCacheService--use configure() instead
    public Hbase10PartitionCache(){ }

    @Override
    public void invalidate(TableName tableName) throws IOException{
        partitionCache.invalidate(tableName);
        ((HConnection)HBaseConnectionFactory.getInstance(config).getConnection()).clearRegionCache(tableName);
    }

    @Override
    public void invalidate(byte[] tableName) throws IOException{
        invalidate(tableInfoFactory.getTableInfo(tableName));
    }

    @Override
    public void configure(SConfiguration configuration){
        this.config=configuration;
        this.tableInfoFactory = HBaseTableInfoFactory.getInstance(config);
    }

    @Override
    public List<Partition> getIfPresent(TableName tableName) throws IOException {
        return partitionCache.getIfPresent(tableName);
    }

    @Override
    public void put(TableName tableName, List<Partition> partitions) throws IOException {
        partitionCache.put(tableName,partitions);
    }
}

