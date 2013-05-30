package com.splicemachine.derby.impl.load;

import com.google.common.collect.*;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.impl.SITransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class BlockImportJob extends FileImportJob{
    protected BlockImportJob(HTableInterface table, ImportContext context) {
        super(table, context);
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Path path = context.getFilePath();
        FileSystem fs = FileSystem.get(SpliceUtils.config);
        if(!fs.exists(path))
            throw new FileNotFoundException("Unable to find file "
                    + context.getFilePath()+" in FileSystem. Did you put it into HDFS?");

        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] locations = fs.getFileBlockLocations(status,0,status.getLen());

        //get the total number of region servers we have to work with
        HBaseAdmin admin = new HBaseAdmin(SpliceUtils.config);
        int allRegionSize = admin.getClusterStatus().getServersSize();

        byte[] tableBytes = Bytes.toBytes(context.getTableName());
        List<HRegionLocation> regions = getRegionLocations(admin,tableBytes);

        //if we can't find any regions, that's weird. Back off and just import it like
        //any other file
        if(regions.size()<=0)
            return super.getTasks();

        //assign one task per BlockLocation
        Iterator<HRegionLocation> regionCycle = Iterators.cycle(regions);
        Map<BlockImportTask,Pair<byte[],byte[]>> taskMap = Maps.newHashMapWithExpectedSize(locations.length);
        String parentTxnString = getParentTransaction().getTransactionIdString();
        String jobId = getJobId();
        for(BlockLocation location:locations){
            /*
             * We preferentially assign tasks to regions where the region is located on the
             * same server as the location. This gives us the maximum possible guarantee, which
             * is that (assuming HDFS is smart enough) reads will be data-local. We can't guarantee
             * (although we can hope) that our writes will be local as well, and for very small tables (
             * 1 or 2 regions), we can probably get it, but we do the best we can
             *
             * To make sure we spread the locations out among the different regions, we go round-robin through
             * the region locations list
             */
            String[] blockHosts = location.getHosts();
            int length = regions.size();
            int visited = 0;
            boolean found = false;
            while(!found && visited<length){
                HRegionLocation next = regionCycle.next();
                String regionHost = next.getHostname();
                for(String blockHost:blockHosts){
                    if(regionHost.equalsIgnoreCase(blockHost)){
                        BlockImportTask task = new BlockImportTask(
                                jobId,
                                context,
                                location,
                                SpliceConstants.DEFAULT_IMPORT_TASK_PRIORITY,
                                parentTxnString,
                                false);
                        Pair<byte[],byte[]> regionBounds = Pair.newPair(next.getRegionInfo().getStartKey(),
                                next.getRegionInfo().getEndKey());
                        taskMap.put(task,regionBounds);
                        found=true;
                        break;
                    }
                }
                visited++;
            }
            if(!found){
                /*
                 * There are no regions which are data-local to this block. In really
                 * large tables, we could probably wait around and re-check after the
                 * first round is through, but for simplicity's sake, we'll just assign
                 * it to the next available region. Here we'll have remote reads,
                 * but the hope is that we'll get at least some local writes and the
                 * whole thing won't suck horrendously.
                 */
                BlockImportTask task = new BlockImportTask(
                        jobId,
                        context,
                        location,
                        SpliceConstants.DEFAULT_IMPORT_TASK_PRIORITY,
                        parentTxnString,
                        false);
                HRegionLocation next = regionCycle.next();
                Pair<byte[],byte[]> regionBounds = Pair.newPair(next.getRegionInfo().getStartKey(),
                        next.getRegionInfo().getEndKey());
                taskMap.put(task,regionBounds);
            }
        }
        return taskMap;
    }

    private List<HRegionLocation> getRegionLocations(HBaseAdmin admin,byte[] tableBytes)
            throws IOException {
		/*
		 * Get the RegionLocations for a table based on the HRegionInfos returned.
		 */
        List<HRegionInfo> tableRegions = admin.getTableRegions(tableBytes);

        List<HRegionLocation> regions = Lists.newArrayListWithCapacity(tableRegions.size());
        for (HRegionInfo tableRegion : tableRegions) {
            HRegionLocation loc = admin.getConnection().locateRegion(tableRegion.getTableName(),tableRegion.getStartKey());
            if(loc!=null)
                regions.add(loc);

        }
        return regions;
    }

    public static void main(String... args) throws Exception{
        InetAddress localhost = InetAddress.getByName("localhost");
        System.out.println(localhost.getHostAddress());
        InetAddress localIp = InetAddress.getByAddress(new byte[]{10,0,0,16});
        System.out.printf("localIp=%s, localIp.getHostAddress()=%s%n",localIp,localIp.getHostAddress());


    }
}
