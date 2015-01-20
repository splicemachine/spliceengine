package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.Counter;

public class BaseHRegionUtil {
    public static void updateWriteRequests(HRegion region, long numWrites){
        Counter writeRequestsCount = region.writeRequestsCount;
        if(writeRequestsCount!=null)
            writeRequestsCount.add(numWrites);
    }

    public static void updateReadRequests(HRegion region, long numReads){
        Counter readRequestsCount = region.readRequestsCount;
        if(readRequestsCount!=null)
            readRequestsCount.add(numReads);
    }

    public static boolean containsRange(HRegionInfo region, byte[] taskStart, byte[] taskEnd) {
        byte[] regionStart = region.getStartKey();

        if(regionStart.length!=0){
            if(taskStart.length==0) return false;
            if(taskEnd.length!=0 && Bytes.compareTo(taskEnd,taskStart)<=0) return false; //task end is before region start

            //make sure taskStart >= regionStart
            if(Bytes.compareTo(regionStart,taskStart)>0) return false; //task start is before region start
        }

        byte[] regionStop = region.getEndKey();
        if(regionStop.length!=0){
            if(taskEnd.length==0) return false;
            if(taskStart.length!=0 && Bytes.compareTo(taskStart,regionStop)>=0) return false; //task start is after region stop

            if(Bytes.compareTo(regionStop,taskEnd)<0) return false; //task goes past end of region
        }
        return true;
    }

    public static boolean containsRange(HRegion region, byte[] taskStart, byte[] taskEnd) {
        byte[] regionStart = region.getStartKey();

        if(regionStart.length!=0){
            if(taskStart.length==0) return false;
            if(taskEnd.length!=0 && Bytes.compareTo(taskEnd,taskStart)<=0) return false; //task end is before region start

            //make sure taskStart >= regionStart
            if(Bytes.compareTo(regionStart,taskStart)>0) return false; //task start is before region start
        }

        byte[] regionStop = region.getEndKey();
        if(regionStop.length!=0){
            if(taskEnd.length==0) return false;
            if(taskStart.length!=0 && Bytes.compareTo(taskStart,regionStop)>=0) return false; //task start is after region stop

            if(Bytes.compareTo(regionStop,taskEnd)<0) return false; //task goes past end of region
        }

        return true;
    }

    /**
     * Determines if the specified row is within the row range specified by the
     * specified HRegionInfo
     *
     * @param info HRegionInfo that specifies the row range
     * @param row row to be checked
     * @return true if the row is within the range specified by the HRegionInfo
     */
    public static boolean containsRow(HRegionInfo info,byte[] row, int rowOffset,int rowLength){
        byte[] startKey = info.getStartKey();
        byte[] endKey = info.getEndKey();
        return ((startKey.length == 0) ||
                (Bytes.compareTo(startKey,0,startKey.length, row,rowOffset,rowLength) <= 0)) &&
                ((endKey.length == 0) ||
                        (Bytes.compareTo(endKey,0,endKey.length,row,rowOffset,rowLength) > 0));
    }
    
    public static RegionScanner getScanner(HRegion region, Scan scan, List<KeyValueScanner> keyValueScanners) throws IOException {
    	   return region.getScanner(scan, keyValueScanners);
     }
}
