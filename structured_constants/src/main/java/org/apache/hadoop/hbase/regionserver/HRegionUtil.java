package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import com.google.common.io.Closeables;
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
	 * @param region
	 * @param store
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static boolean keyExists(HRegion region, Store store, byte[] key) throws IOException {
	    store.lock.readLock().lock();
	    List<StoreFile> storeFiles;
	    try {
	      storeFiles = store.getStorefiles();
	      for (StoreFile file: storeFiles) {
	    	  if (file.createReader().generalBloomFilter.contains(key, 0, key.length, null))
	    		  return true;
	      }
	      KeyValue kv = new KeyValue(key, HConstants.LATEST_TIMESTAMP);
	      KeyValue placeHolder;
	      if (!store.memstore.kvset.isEmpty()) {
		      SortedSet<KeyValue> kvset = store.memstore.kvset.tailSet(kv);
		      placeHolder = kvset.isEmpty()?null:kvset.first();
		      if (placeHolder != null && placeHolder.matchingRow(key))
		    	  return true;
	      }
	      if (!store.memstore.snapshot.isEmpty()) {
		      SortedSet<KeyValue> snapshot = store.memstore.snapshot.tailSet(kv);	      
		      placeHolder = snapshot.isEmpty()?null:snapshot.first();
		      if (placeHolder != null && placeHolder.matchingRow(key))
	    		  return true;
	      }
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
	
}
