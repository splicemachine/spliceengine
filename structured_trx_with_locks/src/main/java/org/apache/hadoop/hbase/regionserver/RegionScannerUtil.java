package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
/**
 * 
 * Allows for a hook to attach additional scanners
 * 
 * @author jleach
 *
 */
public class RegionScannerUtil {
	
	public static RegionScanner getRegionScanner(HRegion region, Scan scan, List<KeyValueScanner> additionalScanners) throws IOException {
		return region.getScanner(scan, additionalScanners);
	}
}
