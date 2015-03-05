package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * 
 * 
 */
public class MemStoreFlushAwareScanner extends StoreScanner {
	 protected static final Logger LOG = Logger.getLogger(MemStoreFlushAwareScanner.class);
	   public final static String FLUSH_EVENT = "FLUSH";   	
	   protected AtomicReference<MemstoreAware> memstoreAware;
	   protected MemstoreAware initialValue;
	   protected HRegion region;
	   protected boolean beginRow = true;
	   protected boolean endRowNeedsToBeReturned = false;
	   protected boolean endRowAlreadyReturned = false;
	   protected boolean flushAlreadyReturned = false;
	   protected int counter = 0;
	   SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();
	   
		public MemStoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan, 
				final NavigableSet<byte[]> columns, long readPt, AtomicReference<MemstoreAware> memstoreAware, MemstoreAware initialValue) throws IOException {
			super(store, scanInfo, scan, columns, readPt);
			if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "init for region=%s, scan=%s", region.getRegionNameAsString(),scan);
			this.memstoreAware = memstoreAware;
			this.initialValue = initialValue;
			this.region = region;
		}
		
		@Override
		public KeyValue peek() {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "peek -->" + super.peek());
			if (didWeFlush()) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "already Flushed");
				if (flushAlreadyReturned) {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "returning counter");				
					return new KeyValue(Bytes.toBytes(counter),MRConstants.FLUSH,MRConstants.FLUSH,MRConstants.FLUSH);
				}
				else {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "returning flush");				
					return MRConstants.MEMSTORE_BEGIN_FLUSH;
				}
			}
			if (beginRow)
				return MRConstants.MEMSTORE_BEGIN;
			KeyValue peek = super.peek();
			if (peek == null && !endRowAlreadyReturned) {
				endRowNeedsToBeReturned = true;
				return MRConstants.MEMSTORE_END;
			}
			return super.peek();
		}



		@Override
		public KeyValue next() {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "next");
			throw new RuntimeException("Not Implemented");
		}



		@Override
		public boolean seek(KeyValue key) throws IOException {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "seek with key=%s",key);
			throw new IOException("Not Implemented");
		}


		@Override
		public boolean reseek(KeyValue kv) throws IOException {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "reseek kv=%s",kv);
			throw new IOException("reseek not implemented");
		}

		public boolean internalNext(List<Cell> outResult) throws IOException {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "next kv=%s",outResult);
			if (beginRow) {
				beginRow = false;
				return outResult.add((Cell)MRConstants.MEMSTORE_BEGIN);
			}
			if (endRowNeedsToBeReturned) {
				endRowAlreadyReturned = true;
				return outResult.add((Cell)MRConstants.MEMSTORE_END);
			}
			if (didWeFlush()) {
				if (flushAlreadyReturned) {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "flush already returned");
					counter++;
					outResult.add((Cell)dataLib.newValue(Bytes.toBytes(counter),MRConstants.FLUSH,MRConstants.FLUSH,Long.MAX_VALUE,MRConstants.FLUSH));
				} else {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "Flush has not returned");
						flushAlreadyReturned = true;
						outResult.add((Cell)MRConstants.MEMSTORE_BEGIN_FLUSH);
				}
				return true;
			}
			return directInternalNext(outResult);
		}

		@Override
		public void close() {
			if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "close");
			while (true) {
				MemstoreAware latest = memstoreAware.get();
				if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest)));
					break;
			}
		}
		
		private boolean didWeFlush() {
			return memstoreAware.get().flushCount != initialValue.flushCount;
		}

	
	@Override
	public boolean next(List<Cell> outResult) throws IOException {
		return internalNext(outResult);
	}

	@Override
	public boolean next(List<Cell> outResult, int limit) throws IOException {
		return internalNext(outResult);
	}

	boolean directInternalNext(List<Cell> result) throws IOException {
		return super.next(result,-1);
	}
}
