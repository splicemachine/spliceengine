package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.*;
import com.splicemachine.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.Counter;

/**
 * Class for accessing protected methods in HBase.
 * 
 * @author johnleach
 *
 */
public class HRegionUtil extends BaseHRegionUtil {
    public static KeyExists keyExists;

		public static void lockStore(Store store) {
			((HStore)store).lock.readLock().lock();
		}

		public static void unlockStore(Store store){
			((HStore)store).lock.readLock().unlock();
		}

    public static void startRegionOperation(HRegion region) throws IOException {
        region.startRegionOperation();
    }

    public static boolean lastElementIsLesser(KeyValueSkipListSet skipList, byte[] key) {
        try {
            KeyValue placeHolder = skipList.last();
            if (placeHolder != null && Bytes.compareTo(placeHolder.getBuffer(), placeHolder.getKeyOffset(),
                    placeHolder.getKeyLength(), key, 0, key.length) < 0) { // Skip
                return true;
            }
            return false;
        } catch (NoSuchElementException e) { // Empty KeyValueSkipListSet
            return true;
        }
    }

    /**
     * Tests if the key exists in the memstore (hard match) or in the bloom filters (false positives allowed).  This
     * code is utilized via constraint checking and SI Write/Write conflict checking
     *
     * @param store
     * @param key
     * @return
     * @throws IOException
     */
    public static boolean keyExists(Store store, byte[] key) throws IOException {
        return keyExists.keyExists(store, key);
    }

    protected static boolean checkMemstoreSet(SortedSet<Cell> set, byte[] key, Cell kv) {
        Cell placeHolder;
        try {
            SortedSet<Cell> kvset = set.tailSet(kv);
            placeHolder = kvset.isEmpty() ? null : kvset.first();
            if (placeHolder != null && CellUtil.matchingRow(placeHolder, key))
                return true;
        } catch (NoSuchElementException ignored) {
        } // This keeps us from constantly performing key value comparisons for empty set
        return false;
    }

    public static void closeRegionOperation(HRegion region) throws IOException{
        region.closeRegionOperation();
    }

    public static void updateWriteRequests(HRegion region, long numWrites) {
        Counter writeRequestsCount = region.writeRequestsCount;
        if (writeRequestsCount != null)
            writeRequestsCount.add(numWrites);
    }

    public static void updateReadRequests(HRegion region, long numReads) {
        Counter readRequestsCount = region.readRequestsCount;
        if (readRequestsCount != null)
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

    public static boolean containsRange(HRegion region, byte[] taskStart, byte[] taskEnd) {
        byte[] regionStart = region.getStartKey();

        if (regionStart.length != 0) {
            if (taskStart.length == 0) return false;
            if (taskEnd.length != 0 && Bytes.compareTo(taskEnd, taskStart) <= 0)
                return false; //task end is before region start

            //make sure taskStart >= regionStart
            if (Bytes.compareTo(regionStart, taskStart) > 0) return false; //task start is before region start
        }

        byte[] regionStop = region.getEndKey();
        if (regionStop.length != 0) {
            if (taskEnd.length == 0) return false;
            if (taskStart.length != 0 && Bytes.compareTo(taskStart, regionStop) >= 0)
                return false; //task start is after region stop

            if (Bytes.compareTo(regionStop, taskEnd) < 0) return false; //task goes past end of region
        }

        return true;
    }
    
    public static long getBlocksToRead(Store store, byte[] start, byte[] end) throws IOException {
    	assert Bytes.compareTo(start, end) <= 0 || start.length == 0 || end.length ==0;
	     Collection<StoreFile> storeFiles;
	      storeFiles = store.getStorefiles();
	      HFileBlockIndex.BlockIndexReader fileReader;
	      long sizeOfBlocks = 0;
	      for (StoreFile file: storeFiles) {
	    	  if (file != null) {
	    		  fileReader = file.createReader().getHFileReader().getDataBlockIndexReader();
	    		  int size = fileReader.getRootBlockCount();
	    		  for (int i =0; i<size;i++) {
	    			  byte[] possibleCutpoint = KeyValue.createKeyValueFromKey(fileReader.getRootBlockKey(i)).getRow();
	    			  if ((start.length == 0 || Bytes.compareTo(start, possibleCutpoint) < 0) && (end.length ==0 || Bytes.compareTo(end, possibleCutpoint) > 0)) // Do not include cutpoints out of bounds
	    				  sizeOfBlocks+=fileReader.getRootBlockDataSize(i);
	    		  }
	    	  }  
	      }
	      return sizeOfBlocks;
    }
    
    public static List<byte[]> getCutpoints(Store store, byte[] start, byte[] end) throws IOException {
    	assert Bytes.compareTo(start, end) <= 0 || start.length == 0 || end.length ==0;
	    Collection<StoreFile> storeFiles;
	      storeFiles = store.getStorefiles();
	      HFileBlockIndex.BlockIndexReader fileReader;
	      List<byte[]> cutPoints = new ArrayList<byte[]>();
	      for (StoreFile file: storeFiles) {
	    	  if (file != null) {
	    		  fileReader = file.createReader().getHFileReader().getDataBlockIndexReader();
	    		  int size = fileReader.getRootBlockCount();
                  int blockCounter = 0;
	    		  long lastOffset = 0;
	    		  for (int i =0; i<size;i++) {
                      blockCounter += fileReader.getRootBlockOffset(i) - lastOffset;                
	    			  byte[] possibleCutpoint = KeyValue.createKeyValueFromKey(fileReader.getRootBlockKey(i)).getRow();
	    			  if ((start.length == 0 || Bytes.compareTo(start, possibleCutpoint) < 0) && (end.length ==0 || Bytes.compareTo(end, possibleCutpoint) > 0)) { // Do not include cutpoints out of bounds
                          if (blockCounter >= SIConstants.splitBlockSize) {
                        	  lastOffset = fileReader.getRootBlockOffset(i);
                              blockCounter = 0;
                              cutPoints.add(possibleCutpoint); // Will have to create rowKey anyway for scan...
                          }
                      }
	    		  }
	    	  }  
	      }
	      if (storeFiles.size() > 1) { 	    	  // have to sort, hopefully will not happen a lot if major compaction is working properly...
	    	  Collections.sort(cutPoints, new Comparator<byte[]>() {
				@Override
				public int compare(byte[] left, byte[] right) {
					return Bytes.compareTo(left, right) ;
				}});
	      }
	      return cutPoints;
    }
    
    static {
        keyExists = new LogNKeyExists();
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

    public interface KeyExists {
        public boolean keyExists(Store store, byte[] key) throws IOException;    	
    }

    static class LogNKeyExists implements KeyExists {
        // TODO: jc - using package access classes, methods and instance members is advised against
        @Override
        public boolean keyExists(Store store, byte[] key) throws IOException {
            if (key == null)
                return false;
            if (! (store instanceof HStore)) {
                return false;
            }
            HStore hstore = (HStore)store;
            hstore.lock.readLock().lock();
            Collection<StoreFile> storeFiles;
            try {
                storeFiles = store.getStorefiles();
                /*
                 * Apparently, there's an issue where, when you first start up an HBase instance, if you
                 * call this code directly, you can break. In essence, there are no storefiles, so it goes
                 * to the memstore, where SOMETHING (and I don't know what) causes it to mistakenly return
                 * false,
                 * which tells the writing code that it's safe to write, resulting in some missing Primary Key
                 * errors.
                 *
                 * And in practice, it doesn't do you much good to check the memstore if there are no store
                 * files,
                 * since you'll just have to turn around and check the memstore again when you go to perform
                 * your
                 * get/scan. So may as well save ourselves the extra effort and skip operation if there are no
                  * store
                 * files to check.
                 */
                if (storeFiles.size() <= 0) return true;

                Reader fileReader;
                for (StoreFile file : storeFiles) {
                    if (file != null) {
                        fileReader = file.createReader();
                        if (fileReader.generalBloomFilter != null && fileReader.generalBloomFilter.contains(key, 0,
                                key.length, null))
                            return true;
                    }
                }
                Cell kv = new KeyValue(key,
                        SIConstants.DEFAULT_FAMILY_BYTES,
                        SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                        HConstants.LATEST_TIMESTAMP,
                        HConstants.EMPTY_BYTE_ARRAY);
                return checkMemstore(HBasePrivateUtils.getKvset(hstore), key, kv) || checkMemstore(HBasePrivateUtils.getSnapshot(hstore), key, kv);
            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;
            } finally {
                hstore.lock.readLock().unlock();
            }
        }

        protected boolean checkMemstore(KeyValueSkipListSet kvSet, byte[] key, Cell kv) {
            Cell placeHolder;
            try {
                // FIXME: remove ref to private audience KeyValueSkipListSet and so remove cast
                SortedSet<KeyValue> kvset = kvSet.tailSet((KeyValue)kv);
                placeHolder = kvset.isEmpty() ? null : kvset.first();
                if (placeHolder != null && CellUtil.matchingRow(placeHolder, key))
                    return true;
            } catch (NoSuchElementException ignored) {
            } // This keeps us from constantly performing key value comparisons for empty set
            return false;
        }
    }
}