package com.splicemachine.access.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.StorageConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * Created by jleach on 11/18/15.
 */
public class HBase10TableFactory implements PartitionFactory<TableName>{
    private Connection connection;
    private Clock timeKeeper;
    private long splitSleepIntervalMs;
    private volatile AtomicBoolean initialized = new AtomicBoolean(false);
    private String namespace;
    private byte[] namespaceBytes;
    private HBaseTableInfoFactory tableInfoFactory;
    private Cache<TableName, List<HRegionLocation>> regionCache = CacheBuilder.newBuilder().maximumSize(100).build();

    //must be no-args to support the TableFactoryService
    public HBase10TableFactory(){ }

    @Override
    public void initialize(Clock timeKeeper,SConfiguration configuration) throws IOException{
        if(!initialized.compareAndSet(false,true))
            return; //already initialized by someone else

        this.tableInfoFactory = HBaseTableInfoFactory.getInstance(configuration);
        this.timeKeeper = timeKeeper;
        this.splitSleepIntervalMs = configuration.getLong(StorageConfiguration.TABLE_SPLIT_SLEEP_INTERVAL);
        try{
            connection=HBaseConnectionFactory.getInstance(configuration).getConnection();
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
        this.namespace = configuration.getString(HConfiguration.NAMESPACE);
        this.namespaceBytes =Bytes.toBytes(namespace);

    }

    @Override
    public Partition getTable(TableName tableName) throws IOException{
        return new ClientPartition(connection,tableName,connection.getTable(tableName),timeKeeper);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        return getTable(TableName.valueOf(namespace,name));
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(TableName.valueOf(namespaceBytes,name));
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return new H10PartitionAdmin(connection.getAdmin(),splitSleepIntervalMs,timeKeeper,tableInfoFactory);
    }

    // TODO (wjkmerge): review whether the following two methods from master_dataset are needed any more

    public List<HRegionLocation> getRegions(byte[] tableName, boolean refresh) throws IOException, ExecutionException, InterruptedException {
        return getRegions(tableInfoFactory.getTableInfo(tableName),refresh);
    }

    public List<HRegionLocation> getRegions(TableName tableName,boolean refresh) throws IOException, ExecutionException, InterruptedException{
        List<HRegionLocation> regionLocations;
        if (!refresh) {
             regionLocations = regionCache.getIfPresent(tableName);
             if (regionLocations==null) {
                     regionLocations = connection.getRegionLocator(tableName).getAllRegionLocations();
                      regionCache.put(tableName,regionLocations);
             }
             return regionLocations;
        }
        clearRegionCache(tableName);
        regionCache.invalidate(tableName);
        regionLocations = connection.getRegionLocator(tableName).getAllRegionLocations();
        regionCache.put(tableName,regionLocations);
        return regionLocations;
    }

    public void clearRegionCache(TableName tableName){
        regionCache.invalidate(tableName);
        ((HConnection)connection).clearRegionCache(tableName);
    }


    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
