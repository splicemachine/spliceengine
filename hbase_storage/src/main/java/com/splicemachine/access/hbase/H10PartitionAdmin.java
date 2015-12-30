package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.io.InterruptedIOException;
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
}
