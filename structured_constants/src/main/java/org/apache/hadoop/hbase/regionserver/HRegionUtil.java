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
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

import com.google.common.io.Closeables;

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
	      for (StoreFile file: storeFiles) {
	    	  if (file != null && file.createReader().generalBloomFilter != null && file.createReader().generalBloomFilter.contains(key, 0, key.length, null))
	    		  return true;
	      }
	      KeyValue kv = new KeyValue(key, HConstants.LATEST_TIMESTAMP);
	      if (store.memstore.kvset.contains(kv))
	    	  return true;
	      if (store.memstore.snapshot.contains(kv))
	    	  return true;
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
        region.readRequestsCount.add(numReads);
    }
	
}
