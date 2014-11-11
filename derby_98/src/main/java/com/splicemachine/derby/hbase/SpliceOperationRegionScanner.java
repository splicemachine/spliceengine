package com.splicemachine.derby.hbase;

import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

public class SpliceOperationRegionScanner extends SpliceBaseOperationRegionScanner<Cell> {
		private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);

    public SpliceOperationRegionScanner(final RegionScanner regionScanner, final Scan scan, final HRegion region,
                                        TransactionalRegion txnRegion) throws IOException {
    	super(regionScanner,scan,region,txnRegion);
    }

		@Override
		public boolean next(final List<Cell> results) throws IOException {
			return internalNext(results);
		}

		@Override
		public boolean next(List<Cell> result, int limit) throws IOException {
				throw new RuntimeException("Not Implemented");
		}


		@Override
		public boolean nextRaw(List<Cell> keyValues) throws IOException {
				return nextRaw(keyValues);
		}

		@Override
		public boolean isFilterDone() throws IOException {
		     SpliceLogUtils.trace(LOG,"isFilterDone");
		    return regionScanner.isFilterDone();
		}

		@Override
		public long getMaxResultSize() {
			return regionScanner.getMaxResultSize();
		}

		@Override
		public boolean nextRaw(List<Cell> result, int limit) throws IOException {
			throw new RuntimeException("Not Implemented");
		}

}
