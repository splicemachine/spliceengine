package com.splicemachine.stream.utils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 6/9/15.
 */
public class StreamPartitionUtils {

    public static List<HRegionLocation> getRegionsInRange(Connection connection,TableName tableName,byte[] startRow,byte[] stopRow) throws IOException{
        try(RegionLocator regionLocator=connection.getRegionLocator(tableName)){
            List<HRegionLocation> allRegionLocations=regionLocator.getAllRegionLocations();
            if(startRow==null||startRow.length<=0){
                if(stopRow==null||stopRow.length<=0)
                    return allRegionLocations; //we are asking for everything
            }
            List<HRegionLocation> inRange = new ArrayList<>(allRegionLocations.size());
            if(startRow==null||startRow.length<=0){
                //we only need to check if the start of the region occurs before the end of the range
                for(HRegionLocation loc : allRegionLocations){
                    HRegionInfo regionInfo=loc.getRegionInfo();
                    byte[] start = regionInfo.getStartKey();
                    if(start==null||start.length<=0 || Bytes.compareTo(start,stopRow)<0){
                        inRange.add(loc);
                    }
                }
            }else if(stopRow==null||stopRow.length<=0){
                //we only need to check that the end of the region occurs after the start key
                for(HRegionLocation loc:allRegionLocations){
                    byte[] stop = loc.getRegionInfo().getEndKey();
                    if(stop==null||stop.length<=0||Bytes.compareTo(startRow,stop)<0)
                        inRange.add(loc);
                }
            }else{
                //we know that both are not null, so we look for overlapping ranges--startRow <= region.start <stopRow
                //or startRow<region.end<=stopRow
                for(HRegionLocation loc : allRegionLocations){
                    HRegionInfo regionInfo=loc.getRegionInfo();
                    byte[] start = regionInfo.getStartKey();
                    byte[] stop = regionInfo.getEndKey();
                    if(start==null||start.length<=0){
                        if(stop==null||stop.length<=0){
                            //it contains the entire range, so it must contain us
                            inRange.add(loc);
                        }else if(Bytes.compareTo(startRow,stop)<0){
                            //we know that the start of the region is before our start, but we can
                            //still overlap if startRow < stop
                            inRange.add(loc);
                        }
                    }else if(stop==null||stop.length<=0){
                        if(Bytes.compareTo(start,stopRow)<0)
                            inRange.add(loc);
                    }else{
                        if(Bytes.compareTo(stop,startRow)>0){
                            if(Bytes.compareTo(stopRow,start)>0){
                                inRange.add(loc);
                            }
                        }
                    }
                }
            }
            return inRange;
        }
    }
}
