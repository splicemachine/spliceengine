package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.ConcurrentRingBuffer;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Buffers Region writes.
 *
 * There are two scalability concerns with the raw HBase RegionScanner.
 *
 * The first is that reads aren't buffered, which means each read must access the Memstore directly,
 * which may require additional IO (disk seeks, etc.).
 *
 * The second is that each call to regionScanner.next() involves a synchronization step--a read lock
 * must be acquired by the region, and then released. Since we may potentially be doing this over a
 * large portion of the table, this results in a large number of locks and unlocks.
 *
 * One naive way to resolve the locking issue is to just acquire a read lock with the first row in
 * the scan, then release it after the last read has completed. However, this causes additional
 * scalability issues: the read lock that is acquired will block any attempts to close that
 * region until the scan has completed, which may be a very long time. This will prevent splits,
 * and generally hurt overall stability
 *
 * So, to recap: we must have a region scanner which has the following properties:
 *
 * 1. Buffering (hold a buffer of results in memory so that repeated access is not required)
 * 2. Does not hold a region lock for longer than is necessary.
 *
 * This class provides such an implementation.
 *
 * @author Scott Fines
 * Created on: 7/25/13
 */
public class BufferedRegionScanner implements MeasuredRegionScanner{
		private final HRegion region;
		private final RegionScanner delegate;

		//buffer filling stuff
		private List<KeyValue>[] buffer;
		private int bufferPosition;
		private final int maxBufferSize;

		//statistics information
		private final Timer readTimer;
		private final Counter bytesReadCounter;

		public BufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 int bufferSize,
																 MetricFactory metricFactory) {
			this(region,delegate,bufferSize,16,metricFactory); //initial buffer size of 16
		}

		public BufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 int maxBufferSize,
																 int  initialBufferSize,
																 MetricFactory metricFactory) {
				this.region = region;
				this.delegate = delegate;

				this.bufferPosition =0;
				int s=1;
				while(s<maxBufferSize){
						s<<=1;
				}
				this.maxBufferSize = s;
				if(initialBufferSize>maxBufferSize)
						initialBufferSize = this.maxBufferSize; //don't create more entries than we allow in the entire buffer
				else{
						s = 1;
						while(s<initialBufferSize){
								s<<=1;
						}
						initialBufferSize = s;
				}
				//noinspection unchecked
				this.buffer = (List<KeyValue>[])new List[initialBufferSize];
				this.readTimer = metricFactory.newTimer();
				this.bytesReadCounter = metricFactory.newCounter();
		}

		@Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
		@Override public boolean isFilterDone() { return delegate.isFilterDone(); }
		@Override public boolean reseek(byte[] row) throws IOException { return delegate.reseek(row); }
		@Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }
		@Override public boolean nextRaw(List<KeyValue> result, String metric) throws IOException { return next(result); }
		@Override public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException { return next(result); }

		@Override
		public boolean next(List<KeyValue> results) throws IOException {
       /*
        * The basic HBase next(results) method generally does the following steps:
        *
        * 1. acquire a read lock
        * 2. check if region is closed (exploding if it is)
        * 3. set the thread read point
        * 4. do some reading of data
        * 5. release the lock
        *
        * through the HBase api, this looks like
        *
        * region.startRegionOperation();
        * try{
        *   MultiVersionConsistencyControl.setThreadReadPoint(regionScanner.getMvccReadPoint());
        *
        *  //read data using regionScanner.nextRaw();
        * }finally{
        *  region.stopRegionOperation();
        * }
        *
        * We wish to do the same thing, but only when our buffer is exhausted. This is done
        * in the refill() method. To avoid overusing memory for small scans, we initialize the
        * buffer to smaller than maxBufferSize, and then expand it until meeting the limit.
        */
				if(bufferPosition==0)
						refill();

				List<KeyValue> next = buffer[bufferPosition];
				if(next!=null){
						results.addAll(next);
						bufferPosition = (bufferPosition+1)&(buffer.length-1);
						return true;
				}else return false;
		}

		@Override public boolean next(List<KeyValue> results, String metric) throws IOException { return next(results); }
		@Override public boolean next(List<KeyValue> result, int limit) throws IOException { return next(result); }
		@Override public boolean next(List<KeyValue> result, int limit, String metric) throws IOException { return next(result); }
		@Override public void close() throws IOException { delegate.close(); }

		private void refill() throws IOException{
				region.startRegionOperation();
				MultiVersionConsistencyControl.setThreadReadPoint(delegate.getMvccReadPoint());
				int bufferPos = 0;
				readTimer.startTiming();
				try{
						while(bufferPos<maxBufferSize){
								if(bufferPos>=buffer.length){
										buffer = Arrays.copyOf(buffer,Math.min(maxBufferSize,2*buffer.length));
								}
								List<KeyValue> kvs = buffer[bufferPos];
								if(kvs==null){
										kvs = Lists.newArrayListWithCapacity(2);
										buffer[bufferPos] = kvs;
								}else{
										kvs.clear();
								}

								delegate.nextRaw(kvs,null);
								if(kvs.isEmpty()){
										buffer[bufferPos] =null;
										return;
								}else{
										/*
										 * Normally, we don't need to check isActive(), but in this
										 * case it avoids doing a loop through the kvs if we do
										 * the check.
										 */
										if(bytesReadCounter.isActive()){
												for (KeyValue kv : kvs) { //TODO -sf- does this create an extra object?
														bytesReadCounter.add(kv.getLength());
												}
										}
										bufferPos++;
								}
						}
				}finally{
						region.closeRegionOperation();
						HRegionUtil.updateReadRequests(region, bufferPos);
						readTimer.tick(bufferPos); //the number of records read during this time
				}
		}

		@Override public TimeView getReadTime() { return readTimer.getTime(); }
		@Override public long getBytesRead() { return bytesReadCounter.getTotal(); }

		@Override public long getRowsRead() { return readTimer.getNumEvents(); }
}
