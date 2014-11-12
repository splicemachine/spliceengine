package com.splicemachine.hbase;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author Scott Fines
 * Created on: 7/25/13
 */
public class BufferedRegionScanner extends AbstractBufferedRegionScanner<Put,Get,Cell>{
    private static final Logger LOG = Logger.getLogger(BufferedRegionScanner.class);


		public BufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 Scan scan,
																 int bufferSize,
																 MetricFactory metricFactory, SDataLib<Cell,Put,Delete,Get,Scan> dataLib) {
			super(region,delegate,scan,bufferSize,16,metricFactory, dataLib); //initial buffer size of 16
		}

		public BufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 Scan scan,
																 int maxBufferSize,
																 int  initialBufferSize,
																 MetricFactory metricFactory,
																 SDataLib<Cell,Put,Delete,Get,Scan> dataLib) {
			super(region,delegate,scan,maxBufferSize,initialBufferSize,metricFactory, dataLib);
		}
		@Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
		@Override public boolean isFilterDone() throws IOException { return delegate.isFilterDone(); }
		@Override public boolean reseek(byte[] row) throws IOException { return delegate.reseek(row); }
		@Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }
		@Override public boolean nextRaw(List<Cell> result) throws IOException { return next(result); }
		@Override public boolean nextRaw(List<Cell> result, int limit) throws IOException { return next(result); }
		@Override
		public Cell next() throws IOException {
			return internalNext();
		}
		
		@Override
		public boolean next(List<Cell> results) throws IOException {
			return internalNext(results);
		}
		@Override public boolean next(List<Cell> result, int limit) throws IOException { return next(result); }

		@Override
		public long getMaxResultSize() {
			return delegate.getMaxResultSize();
		}

		@Override
		public boolean internalNextRaw(List<Cell> results) throws IOException {
			// TODO Auto-generated method stub
			return delegate.nextRaw(results);
		}
}
