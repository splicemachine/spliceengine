package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.Store.ScanInfo;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * 
 * 
 */
public class MemStoreFlushAwareScanner extends StoreScanner{
   protected static final Logger LOG = Logger.getLogger(MemStoreFlushAwareScanner.class);
   public final static String FLUSH_EVENT = "FLUSH";   	
   protected AtomicBoolean splitMerge;
   protected AtomicInteger flushCount;
   protected AtomicInteger compactionCount;   
   protected AtomicInteger scannerCount;   
   protected int initialFlushCount;
   protected int initialCompactionCount;
   protected HRegion region;
   protected boolean beginRow = true;
   protected boolean endRowNeedsToBeReturned = false;
   protected boolean endRowAlreadyReturned = false;
   protected boolean flushAlreadyReturned = false;
   

	public MemStoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan, 
			final NavigableSet<byte[]> columns, long readPt, AtomicBoolean splitMerge, AtomicInteger flushCount,int initialFlushCount, AtomicInteger compactionCount, int initialCompactionCount, AtomicInteger scannerCount) throws IOException {
		super(store, scanInfo, scan, columns);
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "init");
		this.splitMerge = splitMerge;
		this.flushCount = flushCount;
		this.initialFlushCount = initialFlushCount;
		this.compactionCount = compactionCount;
		this.initialCompactionCount = initialCompactionCount;
		this.region = region;
		this.scannerCount = scannerCount;
	}

	
	
	@Override
	public KeyValue peek() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "peek -->" + super.peek());
		if (didWeFlush() && !flushAlreadyReturned)
			return MRConstants.MEMSTORE_BEGIN_FLUSH;
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
//		return super.next();
	}



	@Override
	public boolean seek(KeyValue key) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "seek with key=%s",key);
		throw new IOException("Not Implemented");
		//return super.seek(key);
	}



	@Override
	public boolean next(List<KeyValue> outResult) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with passed result=%s",outResult);
		if (!didWeFlush())
			return super.next(outResult);
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "writing flush data with kv=%s",outResult);			
		if (outResult.size()>0 && Bytes.equals(outResult.get(0).getFamily(),MRConstants.FLUSH))
			return false;
		outResult.add(new KeyValue(HConstants.EMPTY_START_ROW,MRConstants.FLUSH,MRConstants.FLUSH));
		return true;
	}



	@Override
	public boolean reseek(KeyValue kv) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "reseek kv=%s",kv);
		throw new IOException("reseek not implemented");
//		return super.reseek(kv);
	}



	@Override
	public boolean next(List<KeyValue> outResult, int limit) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next kv=%s, limit=%d",outResult,limit);
		if (beginRow) {
			beginRow = false;
			return outResult.add(MRConstants.MEMSTORE_BEGIN);
		}
		if (endRowNeedsToBeReturned) {
			endRowAlreadyReturned = true;
			return outResult.add(MRConstants.MEMSTORE_END);
		}
		if (didWeFlush() && !flushAlreadyReturned) {
			flushAlreadyReturned = true;
			return outResult.add(MRConstants.MEMSTORE_BEGIN_FLUSH);
		}
		return super.next(outResult, limit);
	}

	@Override
	public void close() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
		scannerCount.getAndDecrement();		
		Thread.dumpStack();
	}
	
	private boolean didWeFlush() {
		return flushCount.get() != initialFlushCount;
	}
}
