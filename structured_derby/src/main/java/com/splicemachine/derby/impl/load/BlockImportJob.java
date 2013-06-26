package com.splicemachine.derby.impl.load;

import com.google.common.collect.*;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.BetterHTablePool;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

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

        byte[] tableBytes = Bytes.toBytes(context.getTableName());
        Map<ServerName,HRegionInfo> regions = getRegionLocations();
//        List<HRegionLocation> regions = getRegionLocations(admin,tableBytes);

        //if we can't find any regions, that's weird. Back off and just import it like
        //any other file
        if(regions.size()<=0)
            return super.getTasks();

        //assign one task per BlockLocation
        Iterator<ServerName> regionCycle = Iterators.cycle(regions.keySet());
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

    private void putTask(Map<BlockImportTask, Pair<byte[], byte[]>> taskMap, String parentTxnString, String jobId, BlockLocation location, HRegionInfo next) {
        BlockImportTask task = new BlockImportTask(
                jobId,
                context,
                location,
                SpliceConstants.DEFAULT_IMPORT_TASK_PRIORITY,
                parentTxnString,
                false);
//        byte[] end = next.getEndKey();
//        byte[] endKey;
//        if(end.length>0){
//            endKey = new byte[end.length];
//            System.arraycopy(end,0,endKey,0,end.length);
//            do{
//                BytesUtil.decrementAtIndex(endKey, endKey.length - 1);
//            }while(!HRegion.rowIsInRange(next,endKey));
//        }else
//            endKey = end;
        byte[] start = next.getStartKey();
        Pair<byte[],byte[]> regionBounds = Pair.newPair(start, start); //should guarantee only one region involved
        taskMap.put(task,regionBounds);
    }

    private Map<ServerName,HRegionInfo> getRegionLocations() throws IOException{
        NavigableMap<HRegionInfo,ServerName> regionLocations;
        if(table instanceof HTable)
            regionLocations = ((HTable)table).getRegionLocations();
        else if(table instanceof BetterHTablePool.ReturningHTable){
            regionLocations = ((BetterHTablePool.ReturningHTable)table).getDelegate().getRegionLocations();
        }else{
            throw new IOException("Unexpected Table type: " + table.getClass());
        }

        //create a map from Server to a SINGLE region
        Map<ServerName,HRegionInfo> regionsToReturn = new HashMap<ServerName,HRegionInfo>();
        for(HRegionInfo info:regionLocations.keySet()){
            ServerName serverName = regionLocations.get(info);
            HRegionInfo existing = regionsToReturn.get(serverName);
            if(existing!=null){
                //accept the tightest region
                if(BytesUtil.emptyBeforeComparator.compare(existing.getStartKey(),info.getStartKey())==0){
                    if(BytesUtil.emptyBeforeComparator.compare(existing.getEndKey(),info.getStartKey())<=0){
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

    private List<HRegionLocation> getRegionLocations(HBaseAdmin admin,byte[] tableBytes)
            throws IOException {
		/*
		 * Get the RegionLocations for a table based on the HRegionInfos returned.
		 */
        NavigableMap<HRegionInfo,ServerName> regionLocations;
        if(table instanceof HTable){
            regionLocations = ((HTable) table).getRegionLocations();
        }else if(table instanceof BetterHTablePool.ReturningHTable){
            regionLocations = ((BetterHTablePool.ReturningHTable)table).getDelegate().getRegionLocations();
        }else{
            throw new IOException("Unexpected Table Type:" + table.getClass());
        }

        List<HRegionLocation> regions = Lists.newArrayListWithCapacity(regionLocations.size());
        for (HRegionInfo tableRegion : regionLocations.keySet()) {
            ServerName serverName = regionLocations.get(tableRegion);
            regions.add(new HRegionLocation(tableRegion,serverName.getHostname(),serverName.getPort()));
        }

        /*
         * It's possible that admin.getTableRegions() will return incorrect regions. particularly in a split
         * situation, it's possible that the call will return both the parent and the child regions (e.g.
         * you may see [a,b) AND [a,a1) AND [a2,b) for a < a1 < a2 < b). Because we only want to assign
         * ONE region for each task, we need to filter those tasks out. Simple enough, just sort by start key
         * and end key (asc start, descending end), then remove duplicates.
         */
        Collections.sort(regions, new Comparator<HRegionLocation>() {
            @Override
            public int compare(HRegionLocation o1, HRegionLocation o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    else return -1;
                }else if(o2==null) return 1;

                HRegionInfo left = o1.getRegionInfo();
                HRegionInfo right = o2.getRegionInfo();
                int compare = BytesUtil.emptyBeforeComparator.compare(left.getStartKey(),right.getStartKey());
                if(compare !=0) return compare; //sort by start key ascending
                else {
                    //start keys match, sort by end key descending
                    compare = BytesUtil.emptyBeforeComparator.compare(left.getEndKey(),right.getEndKey());
                    if(compare!=0)
                        return -1*compare;
                    else
                        return compare;
                }
            }
        });
        for(int i=0;i<regions.size();i++){
            HRegionLocation next = regions.get(i);
            for(int j=i+1;j<regions.size();j++){
                HRegionLocation possibleDuplicate = regions.get(j);
                if(BytesUtil.emptyBeforeComparator.compare(
                        next.getRegionInfo().getStartKey(),
                        possibleDuplicate.getRegionInfo().getStartKey())==0){
                    regions.remove(j);
                    j--;
                }
            }
        }

        return regions;
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
            byte[] bits2 = BytesUtil.copyAndIncrement(bits);
            System.out.println(Bytes.toStringBinary(Encoding.encode(Bytes.toString(bits))));
            System.out.println(Bytes.toStringBinary(Encoding.encode(Bytes.toString(bits2))));
            System.out.println("");
        }

    }
}
