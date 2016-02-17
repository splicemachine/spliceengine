package com.splicemachine.access.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
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
    private PartitionInfoCache<TableName> partitionInfoCache;

    //must be no-args to support the TableFactoryService
    public HBase10TableFactory(){ }

    @Override
    public void initialize(Clock timeKeeper,SConfiguration configuration, PartitionInfoCache<TableName> partitionInfoCache) throws IOException{
        if(!initialized.compareAndSet(false,true))
            return; //already initialized by someone else
        this.partitionInfoCache = partitionInfoCache;
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
        return new ClientPartition(connection,tableName,connection.getTable(tableName),timeKeeper,partitionInfoCache);
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
        return new H10PartitionAdmin(connection.getAdmin(),splitSleepIntervalMs,timeKeeper,tableInfoFactory,partitionInfoCache);
    }

    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
