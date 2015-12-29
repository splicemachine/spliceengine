package com.splicemachine.storage;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.hbase.TableName;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class H98PartitionInfoCache implements PartitionInfoCache{

    public void invalidate(TableName tableName){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void invalidate(String tableName){
        invalidate(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
    }

    @Override
    public void invalidate(byte[] tableNameBytes){
        invalidate(HBaseTableInfoFactory.getInstance().getTableInfo(tableNameBytes));
    }
}
