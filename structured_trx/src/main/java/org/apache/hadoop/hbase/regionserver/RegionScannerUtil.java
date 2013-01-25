package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * Allows for a hook to attach additional scanners
 * 
 * @author jleach
 *
 */
public class RegionScannerUtil {
	private static Logger LOG = Logger.getLogger(RegionScannerUtil.class);
	public static RegionScanner getRegionScanner(HRegion region, Scan scan, List<KeyValueScanner> additionalScanners) throws IOException {
		SpliceLogUtils.debug(LOG,"inside RegionScannerUtil getRegionScanner");
		return region.getScanner(scan, additionalScanners);
	}
}
