package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
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
	      KeyValue placeHolder;
	      try { 
		      SortedSet<KeyValue> kvset = store.memstore.kvset.tailSet(kv);
		      placeHolder = kvset.isEmpty()?null:kvset.first();
		      if (placeHolder != null && placeHolder.matchingRow(key))
		    	  return true;
	      } catch (NoSuchElementException e) {} // This keeps us from constantly performing key value comparisons for empty set
		  try {
			  SortedSet<KeyValue> snapshot = store.memstore.snapshot.tailSet(kv);	     
		      placeHolder = snapshot.isEmpty()?null:snapshot.first();
		      if (placeHolder != null && placeHolder.matchingRow(key))
	    		  return true;
		  } catch (NoSuchElementException e) {}	    // This keeps us from constantly performing key value comparisons for empty set
	      return false;  
	    } catch (IOException ioe) {
	    	ioe.printStackTrace();
	    	throw ioe;
	    }
	    finally {
	      store.lock.readLock().unlock();
	    }
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
}
