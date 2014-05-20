package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import com.splicemachine.stats.TimeView;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner extends RegionScanner {

		void start();

		TimeView getReadTime();

		long getBytesOutput();

		KeyValue next() throws IOException;

		long getBytesVisited();

		long getRowsOutput();

		long getRowsFiltered();

		long getRowsVisited();
}
