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
    private final SConfiguration config;
    private final HBaseTableInfoFactory tableInfoFactory;

    public Hbase10PartitionCache(SConfiguration config){
        this.config=config;
        this.tableInfoFactory = new HBaseTableInfoFactory(config);
    }

    public void invalidate(TableName tableName) throws IOException{
        ((HConnection)HBaseConnectionFactory.getInstance(config).getConnection()).clearRegionCache(tableName);
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

