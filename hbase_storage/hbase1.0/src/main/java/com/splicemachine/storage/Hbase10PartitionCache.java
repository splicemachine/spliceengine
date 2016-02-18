package com.splicemachine.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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

