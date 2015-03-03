package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;
import com.splicemachine.async.Bytes;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.api.MemStoreFlushAwareScanner;
import com.splicemachine.mrio.api.SplitScannerDNRIOException;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Region Observer for managing indices and 
 * some other tasks
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends AbstractSpliceIndexObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);
    public AtomicBoolean splitMerge = new AtomicBoolean(false);
    public AtomicInteger flushCount = new AtomicInteger(0);
    public AtomicInteger compactionCount = new AtomicInteger(0);
    public AtomicInteger scannerCount = new AtomicInteger(0);
    
    
    @Override
	public void stop(CoprocessorEnvironment e) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "stop");
		super.stop(e);
	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "start");
		super.start(e);
	}

	@Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
//    	if (LOG.isTraceEnabled())
//    			SpliceLogUtils.trace(LOG, "prePut %s",put);
        if(conglomId>0){        	
            if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null) return;

            //we can't update an index if the conglomerate id isn't positive--it's probably a temp table or something
            byte[] row = put.getRow();
            List<Cell> data = put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES);
            KVPair kv;
            if(data!=null&&data.size()>0){
                byte[] value = CellUtil.cloneValue(data.get(0));
                if(put.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)!=null){
                    kv = new KVPair(row,value, KVPair.Type.UPDATE);
                }else
                    kv = new KVPair(row,value);
            }else{
                kv = new KVPair(row, HConstants.EMPTY_BYTE_ARRAY);
            }
            mutate(e.getEnvironment(), kv, operationFactory.fromWrites(put));
        }
        super.prePut(e, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, Durability durability) throws IOException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "preDelete %s",delete);
        if(conglomId>0){
            if(delete.getAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME)==null){
                KVPair deletePair = KVPair.delete(delete.getRow());
                TxnView txn = operationFactory.fromWrites(delete);
                mutate(e.getEnvironment(), deletePair,txn);
            }
        }
        super.preDelete(e, delete, edit, durability);
    }	
	
	@Override
	public KeyValueScanner preStoreScannerOpen(
			ObserverContext<RegionCoprocessorEnvironment> c, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
			throws IOException {
		if (scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY) != null &&
				Bytes.equals(scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY), SIConstants.TRUE_BYTES)) {			
			if(LOG.isDebugEnabled()){
				SpliceLogUtils.debug(LOG, "preStoreScannerOpen in MR mode %s", 
						c.getEnvironment().getRegion() );
			}
			if(LOG.isDebugEnabled()){
				SpliceLogUtils.debug(LOG, "scan Check Code startKey {value=%s, inRange=%s}, endKey {value=%s, inRange=%s}", 
						scan.getStartRow(), HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), scan.getStartRow()),
						scan.getStopRow(), HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), scan.getStopRow()));
			}
			
			
			// Throw Retry Exception if the region is splitting
			if (splitMerge.get()) {
				throw new SplitScannerDNRIOException();
			} else {
				this.scannerCount.incrementAndGet();
				
				if (!HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), scan.getStartRow()) ||
						!HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), scan.getStartRow())) {
					this.scannerCount.decrementAndGet();
					throw new SplitScannerDNRIOException();
				}
				if (splitMerge.get()) {
					this.scannerCount.decrementAndGet();
					throw new SplitScannerDNRIOException();
				}
			}
			
			InternalScan iscan = new InternalScan(scan);
			iscan.checkOnlyMemStore();
			MemStoreFlushAwareScanner scanner = new MemStoreFlushAwareScanner(c.getEnvironment().getRegion(),store, store.getScanInfo(), iscan, targetCols,
		    	        ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_UNCOMMITTED),splitMerge,flushCount,flushCount.get(), compactionCount, compactionCount.get(), scannerCount);
			return scanner;
		}		
		return super.preStoreScannerOpen(c, store, scan, targetCols, s);
	}

	@Override
	public void preSplitBeforePONR(
			ObserverContext<RegionCoprocessorEnvironment> ctx, byte[] splitKey,
			List<Mutation> metaEntries) throws IOException {
	  	if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "preSplitBeforePONR %s ", ctx.getEnvironment().getRegion() );		
	  	splitMerge.set(true); // Setting the atomic boolean
		super.preSplitBeforePONR(ctx, splitKey, metaEntries);
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e,
			HRegion l, HRegion r) throws IOException {
	  	if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "postSplit %s ", e.getEnvironment().getRegion() );		
		super.postSplit(e, l, r);
	}



	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "preFlush called");
		super.preFlush(e);
	}

	
	
	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner) throws IOException {
		SpliceLogUtils.trace(LOG, "preFlush called on store %s",store);
		return super.preFlush(e, store, scanner);
	}
	
	

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "postFlush called");
		super.postFlush(e);
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
		SpliceLogUtils.trace(LOG, "postFlush called on store %s with file=%s",store, resultFile);
		this.flushCount.getAndIncrement(); // Atomically changes the flush count
		super.postFlush(e, store, resultFile);
	}
	
    @Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "preSplit");
		splitMerge.set(true);
		while (this.scannerCount.get()>0) {
			SpliceLogUtils.warn(LOG, "preSplit Delayed waiting for scanners to complete scannersRemaining=%d",scannerCount.get());
			try {
				Thread.sleep(1000); // Have Split sleep for a second
			} catch (InterruptedException e1) {
				throw new IOException(e1);
			}
		}
    	super.preSplit(e);
	}

	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner, ScanType scanType) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "preCompact store=%s, scanner=%s, scanType=%s",store, scanner, scanType);
		compactionCount.getAndIncrement();
		while (this.scannerCount.get()>0) {
			SpliceLogUtils.warn(LOG, "compaction Delayed waiting for scanners to complete scannersRemaining=%d",scannerCount.get());
			try {
				Thread.sleep(1000); // Have Split sleep for a second
			} catch (InterruptedException e1) {
				throw new IOException(e1);
			}
		}
		
		return super.preCompact(e, store, scanner, scanType);
	}

	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner, ScanType scanType,
			CompactionRequest request) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "preCompact store=%s, scanner=%s, scanType=%s, request=%s",store, scanner, scanType, request);
		compactionCount.getAndIncrement();
		while (this.scannerCount.get()>0) {
			SpliceLogUtils.warn(LOG, "compaction Delayed waiting for scanners to complete scannersRemaining=%d",scannerCount.get());
			try {
				Thread.sleep(1000); // Have Split sleep for a second
			} catch (InterruptedException e1) {
				throw new IOException(e1);
			}
		}
		return super.preCompact(e, store, scanner, scanType, request);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {	
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s",store,resultFile);
		compactionCount.getAndDecrement();		
		super.postCompact(e, store, resultFile);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile, CompactionRequest request)
			throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s, request=%s",store,resultFile, request);
		compactionCount.getAndDecrement();		
		super.postCompact(e, store, resultFile, request);
	}
	
    
    
}
