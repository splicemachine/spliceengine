package com.splicemachine.derby.hbase;


import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class SpliceOperationRegionScanner extends SpliceBaseOperationRegionScanner<KeyValue> {
		private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);

    public SpliceOperationRegionScanner(final RegionScanner regionScanner, final Scan scan, final HRegion region,
                                        TransactionalRegion txnRegion) throws IOException {
    	super(regionScanner,scan,region,txnRegion);
    }

		@Override
		public boolean next(final List<KeyValue> results) throws IOException {
			return internalNext(results);
		}

		@Override
		public boolean next(List<KeyValue> result, int limit) throws IOException {
				throw new RuntimeException("Not Implemented");
		}

		@Override
		public boolean next(List<KeyValue> results, String metric)throws IOException {
				return next(results);
		}

		@Override
		public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
				throw new IOException("next with metric not supported " + metric);
		}


		@Override
		public boolean nextRaw(List<KeyValue> keyValues, String metric) throws IOException {
				return nextRaw(keyValues);
		}

		@Override
		public boolean nextRaw(List<KeyValue> arg0, int arg1, String arg2) throws IOException {
				throw new IOException("Not Implemented");
		}

		public boolean nextRaw(List<KeyValue> keyValues) throws IOException {
				return next(keyValues);
		}

		@Override
		public boolean isFilterDone() {
		     SpliceLogUtils.trace(LOG,"isFilterDone");
		    return regionScanner.isFilterDone();
		}

}
