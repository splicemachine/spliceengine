package com.splicemachine.access.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.LazyPartitionServer;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.RangedClientPartition;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
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

    public H10PartitionAdmin(Admin admin,
                             long splitSleepInterval,
                             Clock timeKeeper){
        this.admin=admin;
        this.splitSleepInterval = splitSleepInterval;
        this.timeKeeper=timeKeeper;
    }

    @Override
    public PartitionCreator newPartition() throws IOException{
        //TODO -sf- configure this more carefully
        HColumnDescriptor snapshot = new HColumnDescriptor(SIConstants.DEFAULT_FAMILY_BYTES);
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.NONE);
        snapshot.setInMemory(true);
        snapshot.setBlockCacheEnabled(true);
        snapshot.setBloomFilterType(BloomType.ROW);
        return new HPartitionCreator(admin.getConnection(),snapshot);
    }

    @Override
    public void deleteTable(String tableName) throws IOException{
        admin.deleteTable(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
    }

    @Override
    public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
        TableName tableInfo=HBaseTableInfoFactory.getInstance().getTableInfo(tableName);
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
        TableName tn =HBaseTableInfoFactory.getInstance().getTableInfo(tableName);
        List<HRegionInfo> tableRegions=admin.getTableRegions(tn);
        List<Partition> partitions=new ArrayList<>(tableRegions.size());
        Connection connection=admin.getConnection();
        Table table = connection.getTable(tn);
        for(HRegionInfo info : tableRegions){
            LazyPartitionServer owningServer=new LazyPartitionServer(connection,info,tn);
            partitions.add(new RangedClientPartition(connection,tn,table,info,owningServer,timeKeeper));
        }
        return partitions;
    }
}
