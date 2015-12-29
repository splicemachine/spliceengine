package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.H98PartitionCreator;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

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
        return new ClientPartition(hti,conn);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        TableName tableName=HBaseTableInfoFactory.getInstance().getTableInfo(name);
        return getTable(tableName);
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        TableName tableName=HBaseTableInfoFactory.getInstance().getTableInfo(name);
        return getTable(tableName);
    }

    @Override
    public PartitionCreator createPartition() throws IOException{
        //TODO -sf- configure this more carefully
        HColumnDescriptor snapshot = new HColumnDescriptor(SIConstants.DEFAULT_FAMILY_BYTES);
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.NONE);
        snapshot.setInMemory(true);
        snapshot.setBlockCacheEnabled(true);
        snapshot.setBloomFilterType(BloomType.ROW);
        return new H98PartitionCreator(connectionPool.getConnection(),snapshot);
    }
}
