package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;

public abstract class AbstractBufferedRegionScanner<T> implements MeasuredRegionScanner<T> {
    private static final Logger LOG = Logger.getLogger(AbstractBufferedRegionScanner.class);
		protected final HRegion region;
		protected final RegionScanner delegate;
		protected final Filter scanFilters;
		//buffer filling stuff
		protected List<T>[] buffer;
		protected int bufferPosition;
		protected final int maxBufferSize;

		//statistics information
		protected final Timer readTimer;
		protected final Counter bytesReadCounter;
		protected HasPredicateFilter hpf;

		public AbstractBufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 Scan scan,
																 int bufferSize,
																 MetricFactory metricFactory) {
			this(region,delegate,scan,bufferSize,16,metricFactory); //initial buffer size of 16
		}

		public AbstractBufferedRegionScanner(HRegion region,
																 RegionScanner delegate,
																 Scan scan,
																 int maxBufferSize,
																 int  initialBufferSize,
																 MetricFactory metricFactory) {
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
		delegate.close();
		buffer = null;
	}
	
}
