package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Class for accessing protected methods in HBase.
 *
 * @author johnleach
 */
public class HRegionUtil extends BaseHRegionUtil{
    private static final Logger LOG=Logger.getLogger(HRegionUtil.class);

    public static void lockStore(Store store){
        ((HStore)store).lock.readLock().lock();
    }

    public static void unlockStore(Store store){
        ((HStore)store).lock.readLock().unlock();
    }

    public static void startRegionOperation(HRegion region) throws IOException{
        region.startRegionOperation();
    }

    protected static boolean checkMemstoreSet(SortedSet<Cell> set,byte[] key,Cell kv){
        Cell placeHolder;
        try{
            SortedSet<Cell> kvset=set.tailSet(kv);
            placeHolder=kvset.isEmpty()?null:kvset.first();
            if(placeHolder!=null && CellUtil.matchingRow(placeHolder,key))
                return true;
        }catch(NoSuchElementException ignored){
        } // This keeps us from constantly performing key value comparisons for empty set
        return false;
    }

    public static void closeRegionOperation(HRegion region) throws IOException{
        region.closeRegionOperation();
    }

    public static void updateWriteRequests(HRegion region,long numWrites){
        HBasePlatformUtils.updateWriteRequests(region,numWrites);
    }

    public static void updateReadRequests(HRegion region,long numReads){
        HBasePlatformUtils.updateReadRequests(region,numReads);
    }


    public static boolean containsRange(HRegionInfo region,byte[] taskStart,byte[] taskEnd){
        byte[] regionStart=region.getStartKey();

        if(regionStart.length!=0){
            if(taskStart.length==0) return false;
            if(taskEnd.length!=0 && Bytes.compareTo(taskEnd,taskStart)<=0)
                return false; //task end is before region start

            //make sure taskStart >= regionStart
            if(Bytes.compareTo(regionStart,taskStart)>0) return false; //task start is before region start
        }

        byte[] regionStop=region.getEndKey();
        if(regionStop.length!=0){
            if(taskEnd.length==0) return false;
            if(taskStart.length!=0 && Bytes.compareTo(taskStart,regionStop)>=0)
                return false; //task start is after region stop

            if(Bytes.compareTo(regionStop,taskEnd)<0) return false; //task goes past end of region
        }
        return true;
    }

    /**
     * Determines if the specified row is within the row range specified by the
     * specified HRegionInfo
     *
     * @param info HRegionInfo that specifies the row range
     * @param row  row to be checked
     * @return true if the row is within the range specified by the HRegionInfo
     */
    public static boolean containsRow(HRegionInfo info,byte[] row,int rowOffset,int rowLength){
        byte[] startKey=info.getStartKey();
        byte[] endKey=info.getEndKey();
        return ((startKey.length==0) ||
                (Bytes.compareTo(startKey,0,startKey.length,row,rowOffset,rowLength)<=0)) &&
                ((endKey.length==0) ||
                        (Bytes.compareTo(endKey,0,endKey.length,row,rowOffset,rowLength)>0));
    }

    public static boolean containsRange(HRegion region,byte[] taskStart,byte[] taskEnd){
        byte[] regionStart=region.getRegionInfo().getStartKey();

        if(regionStart.length!=0){
            if(taskStart.length==0) return false;
            if(taskEnd.length!=0 && Bytes.compareTo(taskEnd,taskStart)<=0)
                return false; //task end is before region start

            //make sure taskStart >= regionStart
            if(Bytes.compareTo(regionStart,taskStart)>0) return false; //task start is before region start
        }

        byte[] regionStop=region.getRegionInfo().getEndKey();
        if(regionStop.length!=0){
            if(taskEnd.length==0) return false;
            if(taskStart.length!=0 && Bytes.compareTo(taskStart,regionStop)>=0)
                return false; //task start is after region stop

            if(Bytes.compareTo(regionStop,taskEnd)<0) return false; //task goes past end of region
        }

        return true;
    }

    public static long getBlocksToRead(Store store,byte[] start,byte[] end) throws IOException{
        assert Bytes.compareTo(start,end)<=0 || start.length==0 || end.length==0;
        Collection<StoreFile> storeFiles;
        storeFiles=store.getStorefiles();
        HFileBlockIndex.BlockIndexReader fileReader;
        long sizeOfBlocks=0;
        for(StoreFile file : storeFiles){
            if(file!=null){
                fileReader=file.createReader().getHFileReader().getDataBlockIndexReader();
                int size=fileReader.getRootBlockCount();
                for(int i=0;i<size;i++){
                    byte[] possibleCutpoint=KeyValue.createKeyValueFromKey(fileReader.getRootBlockKey(i)).getRow();
                    if((start.length==0 || Bytes.compareTo(start,possibleCutpoint)<0) && (end.length==0 || Bytes.compareTo(end,possibleCutpoint)>0)) // Do not include cutpoints out of bounds
                        sizeOfBlocks+=fileReader.getRootBlockDataSize(i);
                }
            }
        }
        return sizeOfBlocks;
    }

    static{
        /*
        try {
			KeyValueSkipListSet test = new KeyValueSkipListSet(KeyValue.COMPARATOR);
			KeyValue keyValue = new KeyValue(Bytes.toBytes("sdf"),Bytes.toBytes("sdf"),Bytes.toBytes("sdf"),
			Bytes.toBytes("sdf"));
			test.lower(keyValue);
			keyExists = new Log1KeyExists();
		} catch (Exception e) {
			e.printStackTrace();
			keyExists = new LogNKeyExists();
		}
		*/
    }

    public interface KeyExists{
    }

    static class LogNKeyExists implements KeyExists{

    }
}