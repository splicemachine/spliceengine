package com.splicemachine.storage;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class Hbase10PartitionCache implements PartitionInfoCache{
    private SConfiguration config;
    private HBaseTableInfoFactory tableInfoFactory;

    //must be a no-args to support the PartitionCacheService--use configure() instead
    public Hbase10PartitionCache(){ }

    public void invalidate(TableName tableName) throws IOException{
        ((HConnection)HBaseConnectionFactory.getInstance(config).getConnection()).clearRegionCache(tableName);
    }

    @Override
    public void configure(SConfiguration configuration){
        this.config=configuration;
        this.tableInfoFactory = HBaseTableInfoFactory.getInstance(config);
    }

    @Override
    public void invalidate(String tableName) throws IOException{
        invalidate(tableInfoFactory.getTableInfo(tableName));
    }

    @Override
    public void invalidate(byte[] tableNameBytes) throws IOException{
        invalidate(tableInfoFactory.getTableInfo(tableNameBytes));
    }
}

