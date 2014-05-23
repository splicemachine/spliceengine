package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.splicemachine.stats.TimeView;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner extends RegionScanner {

		void start();

		TimeView getReadTime();

		long getBytesOutput();

		Cell next() throws IOException;

		long getBytesVisited();

		long getRowsOutput();

		long getRowsFiltered();

		long getRowsVisited();
}
