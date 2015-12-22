package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class HBase98TableFactory implements PartitionFactory<TableName>{
    //-sf- this may not be necessary
    private final HConnectionPool connectionPool;

    public HBase98TableFactory(){
        this.connectionPool = HConnectionPool.defaultInstance();
    }

    @Override
    public Partition getTable(TableName tableName) throws IOException{
        HConnection conn = connectionPool.getConnection();
        HTableInterface hti = new HTable(tableName,conn);
        return new ClientPartition(hti);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        TableName tableName=TableName.valueOf(SIConstants.spliceNamespace,name);
        return getTable(tableName);
    }
}
