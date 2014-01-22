package com.splicemachine.hbase;

import com.splicemachine.stats.Counter;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner extends RegionScanner {

		TimeView getReadTime();

		long getBytesRead();

		long getRowsRead();
}
