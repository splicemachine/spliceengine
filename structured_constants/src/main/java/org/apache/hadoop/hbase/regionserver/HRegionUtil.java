package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
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
		Scan scan = new Scan(get);
		RegionScannerImpl scanner = null;
		try {
			scanner = (RegionScannerImpl) hregion.instantiateRegionScanner(scan, null);
			scanner.nextRaw(keyValues, SchemaMetrics.METRIC_GETSIZE);
		} catch (IOException e) {
			throw e;
		} finally {
			Closeables.close(scanner, false);
		}
	}	
}
