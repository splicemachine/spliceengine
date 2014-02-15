package com.splicemachine.hbase;

import java.io.IOException;

import com.splicemachine.stats.TimeView;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner extends RegionScanner {

		TimeView getReadTime();

		long getBytesRead();

		long getRowsRead();
		
		KeyValue next() throws IOException;
}
