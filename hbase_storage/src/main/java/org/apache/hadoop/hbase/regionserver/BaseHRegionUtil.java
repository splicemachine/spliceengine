package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;

public class BaseHRegionUtil {

    public static RegionScanner getScanner(HRegion region, Scan scan, List<KeyValueScanner> keyValueScanners) throws IOException {
    	   return region.getScanner(scan, keyValueScanners);
     }
}
