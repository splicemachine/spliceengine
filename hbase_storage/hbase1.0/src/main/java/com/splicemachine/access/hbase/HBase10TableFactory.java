package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * Created by jleach on 11/18/15.
 */
public class HBase10TableFactory implements PartitionFactory<TableName>{
    protected Connection connection;
    private static PartitionFactory<TableName> INSTANCE=new HBase10TableFactory();

    public HBase10TableFactory(){
        try{
            connection=HBaseConnectionFactory.getInstance().getConnection();
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
    }

    public static PartitionFactory<TableName> getInstance(){
        return INSTANCE;
    }

    @Override
    public Partition getTable(TableName tableName) throws IOException{
        return new ClientPartition(connection,tableName,connection.getTable(tableName));
    }

    @Override
    public Partition getTable(String name) throws IOException{
        return getTable(TableName.valueOf(SpliceConstants.spliceNamespace,name));
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,name));
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
        return new HPartitionCreator(connection,snapshot);
    }

    public List<HRegionLocation> getRegions(String tableName,boolean refresh) throws IOException, ExecutionException, InterruptedException{
        if(refresh)
            clearRegionCache(TableName.valueOf(SpliceConstants.spliceNamespace,tableName));
        return connection.getRegionLocator(TableName.valueOf(SpliceConstants.spliceNamespace,tableName)).getAllRegionLocations();
    }

    public void clearRegionCache(TableName tableName){
        ((HConnection)connection).clearRegionCache(tableName);
    }


    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
