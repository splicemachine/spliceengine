package com.splicemachine.hbase;

import com.splicemachine.metrics.*;
import com.splicemachine.si.data.api.SDataLib;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * RegionScanner that performs "read-aheads" to reduce the overall latency of a query.
 *
 * In essence, this scanner uses a background thread to perform the actual IO of reading data off of disk, while
 * the primary thread ( the "reader" thread) will wait for the background thread to generate rows.
 *
 * Doing this efficiently involves safe publication between threads. For a low-impact implementation, this
 * class uses the LMAX Disruptor's ring buffer(https://github.com/LMAX-Exchange/disruptor) to publish and consume rows.
 *
 * TODO JL This needs to be revisited...
 *
 * @author Scott Fines
 * Date: 5/8/14
 */
public class ReadAheadRegionScanner extends BaseReadAheadRegionScanner<Put,Get,Cell>{

		public ReadAheadRegionScanner(HRegion region, int bufferSize,RegionScanner delegate,MetricFactory metricFactory,
				SDataLib<Cell, Put, Delete, Get, Scan> dataLib){
			super(region,bufferSize,delegate,metricFactory,dataLib);
		}
		@Override
		public boolean nextRaw(List<Cell> result, int limit) throws IOException {
			return internalNextRaw(result,limit, null);
		}
		@Override public boolean nextRaw(List<Cell> result) throws IOException { return nextRaw(result,2); }
		@Override public boolean next(List<Cell> results) throws IOException { return nextRaw(results,2); }
		@Override public boolean next(List<Cell> result, int limit) throws IOException { return nextRaw(result,limit); }
		@Override
		public long getMaxResultSize() {
			return Long.MAX_VALUE;
		}
		@Override
		public boolean internalNextRaw(List<Cell> results) throws IOException {
			// TODO Auto-generated method stub
			return this.nextRaw(results);
		}
}