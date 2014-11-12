package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import com.splicemachine.metrics.TimeView;

import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface MeasuredRegionScanner<T> extends RegionScanner {

		void start();

		TimeView getReadTime();

		long getBytesOutput();

		T next() throws IOException;
		
		boolean internalNextRaw(List<T> results) throws IOException;

		long getBytesVisited();

		long getRowsOutput();

		long getRowsFiltered();

		long getRowsVisited();
}
