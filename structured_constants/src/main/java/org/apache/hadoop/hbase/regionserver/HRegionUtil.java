package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import com.google.common.io.Closeables;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.Counter;

/**
 * Class for accessing protected methods in HBase.
 * 
 * @author johnleach
 *
 */
public class HRegionUtil {
	public static KeyExists keyExists;
	public interface KeyExists {
		boolean keyExists(Store store, byte[] key) throws IOException;
	}
	
	static {
		
		keyExists = new LogNKeyExists();
		/*
		try {
			KeyValueSkipListSet test = new KeyValueSkipListSet(KeyValue.COMPARATOR);
			KeyValue keyValue = new KeyValue(Bytes.toBytes("sdf"),Bytes.toBytes("sdf"),Bytes.toBytes("sdf"),Bytes.toBytes("sdf"));
			test.lower(keyValue);
			keyExists = new Log1KeyExists();
		} catch (Exception e) {
			e.printStackTrace();
			keyExists = new LogNKeyExists();
		}
		*/
	}

	public static void startRegionOperation(HRegion region) throws IOException {
		region.startRegionOperation();
	}

	public static boolean lastElementIsLesser (KeyValueSkipListSet skipList, byte[] key) {
  	  try {
  		  KeyValue placeHolder = skipList.last();
  		  if (placeHolder != null && Bytes.compareTo(placeHolder.getBuffer(), placeHolder.getKeyOffset(), placeHolder.getKeyLength(), key, 0, key.length) < 0) { // Skip
  			  return true;
  		  }
		return false;
  	  } catch (NoSuchElementException e) { // Empty KeyValueSkipListSet
  		  return true;
  	  }
	}
	
	
    
    
	
	
	/**
	 * 
	 * Tests if the key exists in the memstore (hard match) or in the bloom filters (false positives allowed).  This
	 * code is utilized via constraint checking and SI Write/Write conflict checking
	 * 
	 *
     * @param store
     * @param key
     * @return
	 * @throws IOException
	 */
	public static boolean keyExists(Store store, byte[] key) throws IOException {
		return keyExists.keyExists(store, key);
	}

		protected static boolean checkMemstoreSet(SortedSet<KeyValue> set, byte[] key, KeyValue kv) {
				KeyValue placeHolder;
				try {
						SortedSet<KeyValue> kvset = set.tailSet(kv);
						placeHolder = kvset.isEmpty()?null:kvset.first();
						if (placeHolder != null && placeHolder.matchingRow(key))
								return true;
				} catch (NoSuchElementException ignored) {} // This keeps us from constantly performing key value comparisons for empty set
				return false;
		}

		public static void closeRegionOperation(HRegion region) {
		region.closeRegionOperation();
	}

	public static void populateKeyValues(HRegion hregion, List<KeyValue> keyValues, Get get) throws IOException {
		RegionScannerImpl scanner = null;
		RegionCoprocessorHost coprocessorHost = hregion.getCoprocessorHost();
		try {
			  // pre-get CP hook
		    if (coprocessorHost != null) {
		       if (coprocessorHost.preGet(get, keyValues)) {
		         return;
		       }
		    }
			Scan scan = new Scan(get);
		    scanner = (RegionScannerImpl) hregion.instantiateRegionScanner(scan, null);
			scanner.nextRaw(keyValues, SchemaMetrics.METRIC_GETSIZE);
		} catch (IOException e) {
			throw e;
		} finally {
			Closeables.close(scanner, false);
		}
	    if (coprocessorHost != null) {
	        coprocessorHost.postGet(get, keyValues);
	    }
	}

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
    
    static class Log1KeyExists implements KeyExists {

		@Override
		public boolean keyExists(Store store, byte[] key) throws IOException {
			if (key == null)
				return false;
		    store.lock.readLock().lock();
		    List<StoreFile> storeFiles;
		    try {
		      storeFiles = store.getStorefiles();
		      Reader fileReader;
		      for (StoreFile file: storeFiles) {
		    	  if (file != null) {
		    		  fileReader = file.createReader();
			    	  if (fileReader.generalBloomFilter != null && fileReader.generalBloomFilter.contains(key, 0, key.length, null))
			    		  return true;
		    	  }  
		      }

		      KeyValue kv = new KeyValue(key, HConstants.LATEST_TIMESTAMP);
		      if (store.memstore.kvset.lower(kv) == null && store.memstore.snapshot.lower(kv) == null)
		    	  return false;
		      return true;
		    }
		    finally {
		      store.lock.readLock().unlock();
		    }			
			
		}
    	
    }

    static class LogNKeyExists implements KeyExists {

		@Override
		public boolean keyExists(Store store, byte[] key) throws IOException {
				if (key == null)
						return false;
				store.lock.readLock().lock();
				List<StoreFile> storeFiles;
				try {
						storeFiles = store.getStorefiles();
						/*
						 * Apparently, there's an issue where, when you first start up an HBase instance, if you
						 * call this code directly, you can break. In essence, there are no storefiles, so it goes
						 * to the memstore, where SOMETHING (and I don't know what) causes it to mistakenly return false,
						 * which tells the writing code that it's safe to write, resulting in some missing Primary Key errors.
						 *
						 * And in practice, it doesn't do you much good to check the memstore if there are no store files,
						 * since you'll just have to turn around and check the memstore again when you go to perform your
						 * get/scan. So may as well save ourselves the extra effort and skip operation if there are no store
						 * files to check.
						 */
						if(storeFiles.size()<=0) return true;

						Reader fileReader;
						for (StoreFile file: storeFiles) {
								if (file != null) {
										fileReader = file.createReader();
										if (fileReader.generalBloomFilter != null && fileReader.generalBloomFilter.contains(key, 0, key.length, null))
												return true;
								}
						}
						KeyValue kv = new KeyValue(key,
										SIConstants.DEFAULT_FAMILY_BYTES,
										SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
										0l,HConstants.EMPTY_BYTE_ARRAY);
						kv.setMemstoreTS(HConstants.LATEST_TIMESTAMP);
						return checkMemstore(store.memstore.kvset, key, kv) || checkMemstore(store.memstore.snapshot, key, kv);
				} catch (IOException ioe) {
						ioe.printStackTrace();
						throw ioe;
				}
				finally {
						store.lock.readLock().unlock();
				}
		}

				protected boolean checkMemstore(KeyValueSkipListSet kvSet, byte[] key, KeyValue kv) {
						KeyValue placeHolder;
						try {
								SortedSet<KeyValue> kvset = kvSet.tailSet(kv);
								placeHolder = kvset.isEmpty()?null:kvset.first();
								if (placeHolder != null && placeHolder.matchingRow(key))
										return true;
						} catch (NoSuchElementException ignored) {} // This keeps us from constantly performing key value comparisons for empty set
						return false;
				}
		}
}
