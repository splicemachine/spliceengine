package com.splicemachine.derby.impl.job.load;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
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

import java.io.FileNotFoundException;
import java.io.IOException;
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

        Multimap<HRegionLocation,BlockLocation> regionBlockMap = ArrayListMultimap.create();
        Map<HRegionLocation,Integer> serverLoadMap = Maps.newHashMapWithExpectedSize(regions.size());
        List<BlockLocation> unassignedBlocks = Lists.newArrayListWithExpectedSize(locations.length);
        for(BlockLocation location: locations){
            if(location==null) continue;

            serverLoadMap.clear();

            for(HRegionLocation region:regions){
                String host = region.getHostname();
                for(String blockHost:location.getHosts()){
                    if(blockHost.equals(host)){
                        if(serverLoadMap.get(region)==null)
                            serverLoadMap.put(region,1);
                        else
                            serverLoadMap.put(region,serverLoadMap.get(region)+1);
                    }
                }
            }
            HRegionLocation minLoadedRegion = null;
            int minLoad = Integer.MAX_VALUE;
            for(HRegionLocation server:serverLoadMap.keySet()){
                if(minLoad > serverLoadMap.get(server)){
                    minLoadedRegion = server;
                    minLoad = serverLoadMap.get(server);
                }
            }
            if(minLoadedRegion!=null)
                regionBlockMap.put(minLoadedRegion,location);
            else
                unassignedBlocks.add(location);
        }

        //doll out the unassigned blocks to regions evenly
        Iterator<HRegionLocation> serverIter = regionBlockMap.keySet().iterator();
        for(BlockLocation unassignedBlock:unassignedBlocks){
            if(!serverIter.hasNext()){
                serverIter = regionBlockMap.keySet().iterator();
            }
            regionBlockMap.put(serverIter.next(),unassignedBlock);
        }

        Map<BlockImportTask,Pair<byte[],byte[]>> taskMap = Maps.newHashMap();
        for(HRegionLocation location:regionBlockMap.keySet()){
            Collection<BlockLocation> blocks = Lists.newArrayList(regionBlockMap.get(location));
            BlockImportTask task = new BlockImportTask(context,blocks);
            HRegionInfo info = location.getRegionInfo();
            byte[] start = info.getStartKey();
            byte[] end = info.getEndKey();
            if(end.length>0){
                byte[] endRow = new byte[end.length];
                System.arraycopy(end,0,endRow,0,endRow.length);
                BytesUtil.decrementAtIndex(endRow,endRow.length-1);
                end = endRow;
            }
            taskMap.put(task,Pair.newPair(start,end));
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
            HRegionLocation loc = admin.getConnection().locateRegion(tableRegion.getRegionName());
            if(loc!=null)
                regions.add(loc);

        }
        return regions;
    }
}
