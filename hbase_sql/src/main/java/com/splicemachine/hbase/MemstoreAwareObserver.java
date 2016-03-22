package com.splicemachine.hbase;

import com.splicemachine.access.client.MemStoreFlushAwareScanner;
import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.derby.hbase.*;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
//TODO -sf- fix the concurrency on this thing
public class MemstoreAwareObserver extends BaseRegionObserver implements CompactionObserver,
        SplitObserver,
        FlushObserver,
        StoreScannerObserver{
    private static final Logger LOG = Logger.getLogger(MemstoreAwareObserver.class);
    private AtomicReference<MemstoreAware> memstoreAware =new AtomicReference<>(new MemstoreAware());     // Atomic Reference to memstore aware state handling
    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementCompactionCount(latest)))
                break;
        }
        while (memstoreAware.get().scannerCount>0) {
            SpliceLogUtils.warn(LOG,"compaction Delayed waiting for scanners to complete scannersRemaining=%d",memstoreAware.get().scannerCount);
            try {
                Thread.sleep(1000); // Have Split sleep for a second
            } catch (InterruptedException e1) {
                throw new IOException(e1);
            }
        }
        return scanner;
    }



    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile,CompactionRequest request) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if (memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest)))
                break;
        }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,byte[] splitRow) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if(memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, true)))
                break;
        }
        while (memstoreAware.get().scannerCount>0) {
            SpliceLogUtils.warn(LOG, "preSplit Delayed waiting for scanners to complete scannersRemaining=%d",memstoreAware.get().scannerCount);
            try {
                Thread.sleep(1000); // Have Split sleep for a second
            } catch (InterruptedException e1) {
                throw new IOException(e1);
            }
        }
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e,HRegion l,HRegion r) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if(memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, false)))
                break;
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,InternalScanner scanner) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementFlushCount(latest)))
                break;
        }
        return scanner;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile) throws IOException{
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementFlushCount(latest)))
                break;
        }
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,Store store,Scan scan,NavigableSet<byte[]> targetCols,KeyValueScanner s) throws IOException{
        if (scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY) != null &&
                Bytes.equals(scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY),SIConstants.TRUE_BYTES)) {
            if(LOG.isDebugEnabled()){
                SpliceLogUtils.debug(LOG, "preStoreScannerOpen in MR mode %s",
                        c.getEnvironment().getRegion() );
            }
            if(LOG.isDebugEnabled()){
                SpliceLogUtils.debug(LOG, "scan Check Code scan=%s, startKey {value=%s, inRange=%s}, endKey {value=%s, inRange=%s}",scan ,
                        scan.getStartRow(), startRowInRange(c, scan.getStartRow()),
                        scan.getStopRow(), stopRowInRange(c, scan.getStopRow()));
            }

            // Throw Retry Exception if the region is splitting


            while (true) {
                MemstoreAware currentState = memstoreAware.get();
                if (currentState.splitMerge || currentState.compactionCount>0) {
                    SpliceLogUtils.warn(LOG, "splitting, merging, or active compaction on scan on %s", c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString());
                    throw new DoNotRetryIOException();
                }
                if (memstoreAware.compareAndSet(currentState, MemstoreAware.incrementScannerCount(currentState)));
                break;
            }
            if (!startRowInRange(c, scan.getStartRow()) ||
                    !stopRowInRange(c, scan.getStopRow())) {
                while (true) {
                    MemstoreAware latest = memstoreAware.get();
                    if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest)))
                        break;
                }
                SpliceLogUtils.warn(LOG, "scan missed do to split after task creation beginKey=%s, endKey=%s, region=%s",scan.getStartRow(), scan.getStopRow(),c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString());
                throw new DoNotRetryIOException();
            }

            InternalScan iscan = new InternalScan(scan);
            iscan.checkOnlyMemStore();
            HRegion region=c.getEnvironment().getRegion();
            return new MemStoreFlushAwareScanner(region,store,store.getScanInfo(),iscan,targetCols,getReadpoint(region),memstoreAware,memstoreAware.get());
        }else return s;

    }

    private boolean startRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] startRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), startRow);
    }

    private boolean stopRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] stopRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), stopRow)
                || Bytes.equals(c.getEnvironment().getRegion().getRegionInfo().getEndKey(), stopRow);

    }

    private long getReadpoint(HRegion region){
        return region.getMVCC().memstoreReadPoint();
    }
}
