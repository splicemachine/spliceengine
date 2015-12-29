package com.splicemachine.storage;

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

    public void invalidate(TableName tableName) throws IOException{
        ((HConnection)HBaseConnectionFactory.getInstance().getConnection()).clearRegionCache(tableName);
    }

    @Override
    public void invalidate(String tableName) throws IOException{
        invalidate(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
    }

    @Override
    public void invalidate(byte[] tableNameBytes) throws IOException{
        invalidate(HBaseTableInfoFactory.getInstance().getTableInfo(tableNameBytes));
    }
}

