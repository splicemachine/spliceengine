package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractBufferedRegionScanner<Put extends OperationWithAttributes,Get extends OperationWithAttributes, T> implements MeasuredRegionScanner<T> {
		private static final Logger LOG = Logger.getLogger(AbstractBufferedRegionScanner.class);
		protected final HRegion region;
		protected final RegionScanner delegate;
		protected final Filter scanFilters;
		//buffer filling stuff
		protected List<T>[] buffer;
		protected int bufferPosition;
		protected final int maxBufferSize;
		protected SDataLib<T,Put,Delete,Get,Scan> dataLib;
		//statistics information
		protected final Timer readTimer;
		protected final Counter bytesReadCounter;
		protected HasPredicateFilter hpf;
		private boolean isClosed = false;

		public AbstractBufferedRegionScanner(HRegion region,
																				 RegionScanner delegate,
																				 Scan scan,
																				 int bufferSize,
																				 MetricFactory metricFactory,SDataLib<T,Put,Delete,Get,Scan> dataLib) {
				this(region,delegate,scan,bufferSize,16,metricFactory,dataLib); //initial buffer size of 16
		}

		public AbstractBufferedRegionScanner(HRegion region,
																				 RegionScanner delegate,
																				 Scan scan,
																				 int maxBufferSize,
																				 int  initialBufferSize,
																				 MetricFactory metricFactory,
																				 SDataLib<T,Put,Delete,Get,Scan> dataLib) {
				this.dataLib = dataLib;
				this.region = region;
				this.delegate = delegate;
				if(scan!=null)
						this.scanFilters = scan.getFilter();
				else
						this.scanFilters = null;

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
				this.buffer = (List<T>[])new List[initialBufferSize];
				this.readTimer = metricFactory.newTimer();
				this.bytesReadCounter = metricFactory.newCounter();
				hpf = getSIFilter();
		}

		@Override public TimeView getReadTime() { return readTimer.getTime(); }
		@Override public long getBytesOutput() { return bytesReadCounter.getTotal(); }

		public T internalNext() throws IOException {
				assert !isClosed: "Scanner has already been closed!";
				if(bufferPosition==0)
						refill();
				T keyValue = null;
				List<T> next = buffer[bufferPosition];
				if(next!=null) {
						if (next.isEmpty()) {
								bufferPosition = (bufferPosition+1)&(buffer.length-1);
								keyValue = null;
								return keyValue;
						} else {
								keyValue = next.get(next.size()-1);
								bufferPosition = (bufferPosition+1)&(buffer.length-1);
								return keyValue;
						}
				} else
						return keyValue;

		}

		public boolean internalNext(List<T> results) throws IOException {
				assert !isClosed: "Scanner has already been closed!";
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

				List<T> next = buffer[bufferPosition];
				if(next!=null){
						results.addAll(next);
						next.clear(); // Remove Reference?
						bufferPosition = (bufferPosition+1)&(buffer.length-1);
						return true;
				}else return false;
		}

		@Override public long getRowsOutput() { return readTimer.getNumEvents(); }

		@Override
		public long getRowsFiltered() {
			/*
			 * We hav to look at the region scanner and see if it contains an SIFilterPacked.
			 * If so, we can get the correct number here. Otherwise, we'll just return the number
			 * of rows we output 0 (assume we didn't filter anything).
			 */
				EntryPredicateFilter epf = getEntryPredicateFilter();

				if(epf==null)
						return 0;
				else
						return epf.getRowsFiltered();
		}

		@Override
		public long getBytesVisited() {
				HasPredicateFilter hpf = getSIFilter();
				if(hpf==null) return getBytesOutput();
				return hpf.getBytesVisited();
		}

		@Override
		public long getRowsVisited() {
				EntryPredicateFilter epf = getEntryPredicateFilter();

				if(epf==null)
						return getRowsOutput();
				else
						return epf.getRowsOutput()+epf.getRowsFiltered();
		}

		@Override
		public void start() {
				//no-op
		}

		private EntryPredicateFilter getEntryPredicateFilter() {
				HasPredicateFilter hpf = getSIFilter();
				if(hpf==null) return null;
				return hpf.getFilter();
		}

		private HasPredicateFilter getSIFilter(){
				if(scanFilters==null) return null;
				if(scanFilters instanceof FilterList){
						FilterList fl = (FilterList) scanFilters;
						List<Filter> filters = fl.getFilters();
						for(Filter filter:filters){
								if(filter instanceof HasPredicateFilter){
										return (HasPredicateFilter) filter;
								}
						}
				}else if (scanFilters instanceof HasPredicateFilter)
						return (HasPredicateFilter) scanFilters;

				return null;
		}
		@Override public void close() throws IOException {
				if(isClosed) return;
				isClosed=true;
				delegate.close();
		}

		private void refill() throws IOException{
				region.startRegionOperation();
				dataLib.setThreadReadPoint(delegate);
				int bufferPos = 0;
				readTimer.startTiming();
				try{
						while(bufferPos<maxBufferSize){
								if(bufferPos>=buffer.length){
										buffer = Arrays.copyOf(buffer,Math.min(maxBufferSize,2*buffer.length));
								}
								List<T> kvs = buffer[bufferPos];
								if(kvs==null){
										kvs = Lists.newArrayListWithCapacity(2);
										buffer[bufferPos] = kvs;
								}else{
										kvs.clear();
								}

								dataLib.regionScannerNextRaw(delegate, kvs);
								if(kvs.isEmpty()){
										buffer[bufferPos] =null;
										return;
								}else{
								/*
								 * Normally, we don't need to check isActive(), but in this
								 * case it avoids doing a loop through the kvs if we do
								 * the check.
								 */
										if(bytesReadCounter.isActive() && hpf== null){
												for (T kv : kvs) {
														bytesReadCounter.add(dataLib.getLength(kv));
												}
										}
										bufferPos++;
								}
						}
				}finally{
						region.closeRegionOperation();
						BaseHRegionUtil.updateReadRequests(region, bufferPos);
						readTimer.tick(bufferPos); //the number of records read during this time
				}
		}

}
