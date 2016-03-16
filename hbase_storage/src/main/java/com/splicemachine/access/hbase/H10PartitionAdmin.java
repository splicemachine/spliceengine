package com.splicemachine.access.hbase;

import com.google.common.base.Function;
import com.splicemachine.si.impl.driver.SIDriver;
import org.sparkproject.guava.collect.Collections2;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class H10PartitionAdmin implements PartitionAdmin{
    private final Admin admin;
    private final long splitSleepInterval;
    private final Clock timeKeeper;
    private final HBaseTableInfoFactory tableInfoFactory;
    private final PartitionInfoCache<TableName> partitionInfoCache;

    public H10PartitionAdmin(Admin admin,
                             long splitSleepInterval,
                             Clock timeKeeper,
                             HBaseTableInfoFactory tableInfoFactory,
                             PartitionInfoCache<TableName> partitionInfoCache
                             ){
        this.admin=admin;
        this.splitSleepInterval = splitSleepInterval;
        this.timeKeeper=timeKeeper;
        this.tableInfoFactory = tableInfoFactory;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public PartitionCreator newPartition() throws IOException{
        HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(SIDriver.driver().getConfiguration());
        HColumnDescriptor dataFamily = instance.createDataFamily();
        return new HPartitionCreator(tableInfoFactory,admin.getConnection(),timeKeeper,dataFamily,partitionInfoCache);
    }

    @Override
    public void deleteTable(String tableName) throws IOException{
        admin.disableTable(tableInfoFactory.getTableInfo(tableName));
        admin.deleteTable(tableInfoFactory.getTableInfo(tableName));
    }

    @Override
    public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
        TableName tableInfo=tableInfoFactory.getTableInfo(tableName);
        for(byte[] splitPoint:splitPoints){
            admin.split(tableInfo,splitPoint);
        }
        boolean isSplitting = true;
        while(isSplitting){
            isSplitting=false;
            try {
                List<HRegionInfo> regions = admin.getTableRegions(tableInfo);
                if(regions!=null){
                    for(HRegionInfo region:regions){
                        if(region.isSplit()){
                            isSplitting=true;
                            break;
                        }
                    }
                }else{
                    isSplitting=true;
                }

                timeKeeper.sleep(splitSleepInterval,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
    }

    @Override
    public void close() throws IOException{
        admin.close();
    }

    @Override
    public Collection<PartitionServer> allServers() throws IOException{
        Collection<ServerName> servers=admin.getClusterStatus().getServers();
        return Collections2.transform(servers,new Function<ServerName, PartitionServer>(){
            @Override
            public PartitionServer apply(ServerName input){
                return new HServer(input,admin);
            }
        });
    }

    @Override
    public Iterable<? extends Partition> allPartitions(String tableName) throws IOException{
        TableName tn =tableInfoFactory.getTableInfo(tableName);
        List<HRegionInfo> tableRegions=admin.getTableRegions(tn);
        List<Partition> partitions=new ArrayList<>(tableRegions.size());
        Connection connection=admin.getConnection();
        Table table = connection.getTable(tn);
        for(HRegionInfo info : tableRegions){
            LazyPartitionServer owningServer=new LazyPartitionServer(connection,info,tn);
            partitions.add(new RangedClientPartition(connection,tn,table,info,owningServer,timeKeeper,partitionInfoCache));
        }
        return partitions;
    }

    @Override
    public TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException{
        HTableDescriptor[] hTableDescriptors = admin.getTableDescriptors(tables);
        TableDescriptor[] tableDescriptors = new TableDescriptor[hTableDescriptors.length];
        for (int i = 0; i < hTableDescriptors.length; ++i) {
            tableDescriptors[i] = new HBaseTableDescriptor(hTableDescriptors[i]);
        }
        return tableDescriptors;
    }

    @Override
    public Iterable<TableDescriptor> listTables() throws IOException {

        HTableDescriptor[] hTableDescriptors = admin.listTables();
        TableDescriptor[] tableDescriptors = new TableDescriptor[hTableDescriptors.length];
        for (int i = 0; i < hTableDescriptors.length; ++i) {
            tableDescriptors[i] = new HBaseTableDescriptor(hTableDescriptors[i]);
        }
        List<TableDescriptor> tableDescriptorList = Arrays.asList(tableDescriptors);
        return tableDescriptorList;
    }
}
