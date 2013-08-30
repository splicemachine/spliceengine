package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

import com.google.common.io.Closeables;

public class HRegionUtil {

	public static void startRegionOperation(HRegion region) throws IOException {
		region.startRegionOperation();
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
	
	public static RegionScannerImpl populateKeyValuesScanner(HRegion hregion, List<KeyValue> keyValues, Get get) throws IOException {
		RegionScannerImpl scanner = null;
		RegionCoprocessorHost coprocessorHost = hregion.getCoprocessorHost();
		try {
			  // pre-get CP hook
		    if (coprocessorHost != null) {
		       if (coprocessorHost.preGet(get, keyValues)) {
		    	   throw new IOException("Not possible to short circuit this type of scan");
		       }
		    }
			Scan scan = new Scan(get);
		    scanner = (RegionScannerImpl) hregion.instantiateRegionScanner(scan, null);
			scanner.nextRaw(keyValues, SchemaMetrics.METRIC_GETSIZE);
			return scanner;
		} catch (IOException e) {
			throw e;
		} finally {
			Closeables.close(scanner, false);
		}
	}	

	

		
	public static class MultiGetScanner {
		RegionScannerImpl regionScanner;
		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		public MultiGetScanner (Get get, List<KeyValue> keyValues, HRegion hregion) throws IOException {
			regionScanner = populateKeyValuesScanner(hregion,keyValues,get);
		}
	
		public void populateKeyValues(byte[] row) throws IOException {
			regionScanner.reseek(row);
			regionScanner.nextRaw(keyValues, SchemaMetrics.METRIC_GETSIZE);
		}		

		public void close() throws IOException {
			Closeables.close(regionScanner, false);
		}
		public List<KeyValue> getKeyValues() {
			return keyValues;
		}
		
		public boolean reseek(byte[] row) throws IOException {
	        KeyValue kv = KeyValue.createFirstOnRow(row);
	        // use request seek to make use of the lazy seek option. See HBASE-5520
	        boolean result = regionScanner.storeHeap.requestSeek(kv, true, true);
	        if (regionScanner.joinedHeap != null) {
	          result = regionScanner.joinedHeap.requestSeek(kv, true, true) || result;
	        }
	        return result;
		}
		
		public boolean hasValues() {
			return keyValues != null && keyValues.isEmpty();
		}
		
	}
	
	
	
}
