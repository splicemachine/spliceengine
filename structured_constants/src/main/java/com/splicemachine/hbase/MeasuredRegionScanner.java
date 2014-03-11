package com.splicemachine.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.splicemachine.stats.TimeView;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner extends RegionScanner {

		TimeView getReadTime();

		long getBytesOutput();

		Cell next() throws IOException;

		long getBytesVisited();

		long getRowsOutput();

		long getRowsFiltered();

		long getRowsVisited();
}
