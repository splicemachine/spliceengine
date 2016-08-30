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

