package com.splicemachine.derby.impl.load;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.table.SpliceHTableUtil;
import com.splicemachine.job.Task;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class BlockImportJob extends FileImportJob{
    private final FileSystem fs;

    protected BlockImportJob(HTableInterface table, ImportContext context) throws IOException {
        this(table, context, FileSystem.get(SpliceConstants.config));
    }

    protected BlockImportJob(HTableInterface table, ImportContext context, FileSystem fs){
        super(table,context);
        this.fs = fs;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Path path = context.getFilePath();
        if(!fs.exists(path))
            throw new FileNotFoundException("Unable to find file "
                    + context.getFilePath()+" in FileSystem. Did you put it into HDFS?");

        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] locations = fs.getFileBlockLocations(status,0,status.getLen());

        //get the total number of region servers we have to work with
        Map<ServerName,HRegionInfo> regions = getRegionLocations();

        //if we can't find any regions, that's weird. Back off and just import it like
        //any other file
        if(regions.size()<=0)
            return super.getTasks();

        //assign one task per BlockLocation
        Iterator<ServerName> regionCycle = Iterators.cycle(regions.keySet());
        Map<RegionTask,Pair<byte[],byte[]>> taskMap = Maps.newHashMapWithExpectedSize(locations.length);
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
            int regionCount = regions.size();
            int visited = 0;
            boolean found = false;
            while(!found && visited<regionCount){
                ServerName nextRegionServer = regionCycle.next();
                String regionHost = nextRegionServer.getHostname();
                for(String blockHost:blockHosts){
                    if(regionHost.equalsIgnoreCase(blockHost)){
                        putTask(taskMap, parentTxnString, jobId, location, regions.get(nextRegionServer));
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
                putTask(taskMap, parentTxnString, jobId, location, regions.get(regionCycle.next()));
            }
        }
        return taskMap;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        final HTable hTable = SpliceHTableUtil.toHTable(table);
        if(hTable != null) {
						HRegionLocation location;
						if(taskStartKey.length<=0){
								if(taskEndKey.length>0){
										//force a cache reload to avoid issues with stale caches causing use to submit to multiple regions
										location = hTable.getRegionLocation(taskStartKey,true);
								}else{
										/*
										 * we are attempting to resubmit [{},{}), which kind of sucks,
										 * since a naive approach would just load up all retries on to
										 * a single server. Instead, we want to pick a random region
										 */
										byte[] random = new byte[10];
										new Random(System.currentTimeMillis()).nextBytes(random);
										//get a random region location--force cache reload to avoid stale cache problems with resubmits
										location = hTable.getRegionLocation(random,true);
								}
						}else
								location = hTable.getRegionLocation(taskStartKey,true);

            HRegionInfo info = location.getRegionInfo();
            return Pair.newPair(originalTask,getTaskBoundary(info));
        } else {
            throw new IOException("Unexpected Table type: " + table.getClass());
        }
    }

    private void putTask(Map<RegionTask, Pair<byte[], byte[]>> taskMap, String parentTxnString, String jobId, BlockLocation location, HRegionInfo next) {
        ImportReader reader = new BlockImportReader(location);
        ImportTask task = new ImportTask(jobId,context,reader,SpliceConstants.importTaskPriority,parentTxnString);
        Pair<byte[], byte[]> regionBounds = getTaskBoundary(next);
        taskMap.put(task,regionBounds);
    }

    private Pair<byte[], byte[]> getTaskBoundary(HRegionInfo next) {
        byte[] start = next.getStartKey();
        if(start.length<=0){
            /*
             * If we are working with the start range of the table, we have to be careful, because
             * we would create a range containing the entire table, which would submit an additional
             * task for every region, which is clearly not something that we want.
             *
             * To prevent this, when the start key is empty, use the end key instead. If the end key
             * is also empty, then there can be only one region on the whole table anyway, so we're safe. However,
             * if the end key isn't empty, then just decrement that end key down by one to keep it in the same
             * region, then use the end key as our range.
             */
            byte[] end = next.getEndKey();
            if(end.length>0){
                start = new byte[end.length];
                System.arraycopy(end,0,start,0,end.length);
                BytesUtil.unsignedDecrement(start, start.length - 1);
            }
        }
        return Pair.newPair(start, start);
    }

    private Map<ServerName,HRegionInfo> getRegionLocations() throws IOException{
        NavigableMap<HRegionInfo,ServerName> regionLocations;
        final HTable hTable = SpliceHTableUtil.toHTable(table);
        if(hTable != null) {
            regionLocations = hTable.getRegionLocations();
        } else {
            throw new IOException("Unexpected Table type: " + table.getClass());
        }

        //create a map from Server to a SINGLE region
        Map<ServerName,HRegionInfo> regionsToReturn = new HashMap<ServerName,HRegionInfo>();
        for(HRegionInfo info:regionLocations.keySet()){
            ServerName serverName = regionLocations.get(info);
            HRegionInfo existing = regionsToReturn.get(serverName);
            if(existing!=null){
                //accept the tightest region
                if(BytesUtil.startComparator.compare(existing.getStartKey(),info.getStartKey())==0){
                    if(BytesUtil.startComparator.compare(existing.getEndKey(),info.getStartKey())<=0){
                        //existing is a tighter bound than new one, leave it be
                    }else{
                        //new one has a tighter bound, so replace it
                        regionsToReturn.put(serverName,info);
                    }
                }
                //we don't need to change anything--these regions are disjoint
            }else
                regionsToReturn.put(serverName,info);
        }
        return regionsToReturn;
    }

    public static void main(String... args) throws Exception{
        byte[] bytes = Bytes.toBytesBinary("/937:49::5");
        System.out.println(Bytes.toString(bytes));
        bytes = Bytes.toBytesBinary("937:49::4");
        System.out.println(Bytes.toString(bytes));
        System.out.println("");

        long range = (long)Integer.MAX_VALUE-(long)Integer.MIN_VALUE;
        for(int i=1;i<3;i++){
            int splitPoint = (int)(range*i/3 + Integer.MIN_VALUE);
            String actualSplit = Integer.toString(splitPoint);
            byte[] bits = Bytes.toBytes(actualSplit);
            byte[] bits2 = BytesUtil.unsignedCopyAndIncrement(bits);
            System.out.println(Bytes.toStringBinary(Encoding.encode(Bytes.toString(bits))));
            System.out.println(Bytes.toStringBinary(Encoding.encode(Bytes.toString(bits2))));
            System.out.println("");
        }
    }
}
