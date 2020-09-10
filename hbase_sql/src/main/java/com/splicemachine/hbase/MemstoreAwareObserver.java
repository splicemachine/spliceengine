/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hbase;

import com.splicemachine.access.client.ClientRegionConstants;
import com.splicemachine.access.client.MemStoreFlushAwareScanner;
import com.splicemachine.access.client.MemstoreAware;
import com.splicemachine.compactions.SpliceCompactionRequest;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.DummyScanner;
import com.splicemachine.utils.BlockingProbe;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class MemstoreAwareObserver implements RegionCoprocessor, RegionObserver, Coprocessor {

    private static final Logger LOG = Logger.getLogger(MemstoreAwareObserver.class);
    protected AtomicReference<MemstoreAware> memstoreAware =new AtomicReference<>(new MemstoreAware());
    protected Optional<RegionObserver> optionalRegionObserver = Optional.empty();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        try {
            optionalRegionObserver = Optional.of(this);
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"starting [%s]",((RegionCoprocessorEnvironment) e).getRegion().getRegionInfo().getRegionNameAsString());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        try {
            optionalRegionObserver = Optional.empty();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"stopping [%s]", ((RegionCoprocessorEnvironment) e).getRegion().getRegionInfo().getRegionNameAsString());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {
        try {
            BlockingProbe.blockPreCompact();
            if (!(request instanceof SpliceCompactionRequest)) {
                SpliceLogUtils.error(LOG,"Compaction request must be a SpliceCompactionRequest");
                throw new DoNotRetryIOException();
            }
            SpliceCompactionRequest scr = (SpliceCompactionRequest) request;
            // memstoreAware is injected into the request, where the blocking logic lives, and where compaction
            // count will be incremented and decremented.
            scr.setMemstoreAware(memstoreAware);
            HRegion region = (HRegion) c.getEnvironment().getRegion();
            scr.setRegion(region);
            return scanner == null ? DummyScanner.INSTANCE : scanner;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile,
                            CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try {
            BlockingProbe.blockPostCompact();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner,
                                    FlushLifeCycleTracker tracker) throws IOException {
        try {
            BlockingProbe.blockPreFlush();
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                if(memstoreAware.compareAndSet(latest, MemstoreAware.changeFlush(latest, true)))
                    break;
            }
            return scanner;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile,
                          FlushLifeCycleTracker tracker) throws IOException{
        try {
            BlockingProbe.blockPostFlush();
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                if(memstoreAware.compareAndSet(latest, MemstoreAware.changeFlush(latest, false)))
                    break;
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return optionalRegionObserver;
    }

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner s) throws IOException {
        if (scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY) != null &&
                Bytes.equals(scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY), SIConstants.TRUE_BYTES)) {
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "preStoreScannerOpen in MR mode %s",
                        c.getEnvironment().getRegion());
            }
            HRegion region = (HRegion) c.getEnvironment().getRegion();
            HStore store = region.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
            return postScannerOpenAction(c, store, scan, null, s);
        }
        return s;
    }

    protected RegionScanner postScannerOpenAction(ObserverContext<RegionCoprocessorEnvironment> c,Store store,Scan scan,NavigableSet<byte[]> targetCols, RegionScanner s) throws IOException{
        try {
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

                byte[] startKey = scan.getAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_BEGIN_KEY);
                byte[] endKey = scan.getAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_END_KEY);
                byte[] serverName = scan.getAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_SERVER);


                // Throw Retry Exception if the region is splittingI real

                while (true) {
                    MemstoreAware currentState = memstoreAware.get();
                    if (currentState.splitMerge || currentState.currentCompactionCount>0 || currentState.flush) {
                        String message = "Splitting, merging, or active compaction on scan on " +
                                c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString() +
                                " served by " + c.getEnvironment().getServerName() +
                                " with state" + currentState;
                        SpliceLogUtils.warn(LOG, message);
                        throw new IOException(message);
                    }

                    if (memstoreAware.compareAndSet(currentState, MemstoreAware.incrementScannerCount(currentState)))
                        break;
                }
                if (Bytes.equals(startKey,c.getEnvironment().getRegionInfo().getStartKey()) &&
                        Bytes.equals(endKey,c.getEnvironment().getRegionInfo().getEndKey()) &&
                        Bytes.equals(serverName,Bytes.toBytes(((RegionServerServices)c.getEnvironment().getOnlineRegions()).getServerName().getHostAndPort()))
                        ) {
                    // Partition Hit
                    InternalScan iscan = new InternalScan(scan);
                    iscan.checkOnlyMemStore();
                    HRegion region = (HRegion) c.getEnvironment().getRegion();
                    // We substitute the pre-created scanner, must close it to avoid a memory leak
                    if (s != null)
                        s.close();
                    return new MemStoreFlushAwareScanner(region, store, ((HStore)store).getScanInfo(), iscan, targetCols, getReadpoint(region), memstoreAware, memstoreAware.get());
                } else { // Partition Miss
                    while (true) {
                        MemstoreAware latest = memstoreAware.get();
                        if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest)))
                            break;
                    }
                    String message = String.format("Scan missed due to split after task creation " +
                                    "scan [%s,%s], partition[%s,%s], region=[%s,%s]," +
                                    "server=[%s,%s]",
                            displayByteArray(scan.getStartRow()),
                            displayByteArray(scan.getStopRow()),
                            displayByteArray(startKey),
                            displayByteArray(endKey),
                            displayByteArray(c.getEnvironment().getRegionInfo().getStartKey()),
                            displayByteArray(c.getEnvironment().getRegionInfo().getEndKey()),
                            Bytes.toString(serverName),
                            ((RegionServerServices)c.getEnvironment().getOnlineRegions()).getServerName().getHostAndPort());
                    SpliceLogUtils.warn(LOG, message);

                    throw new DoNotRetryIOException(message);
                }
            }else return s;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }

    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) throws IOException {
        try {
            if (abortRequested) {
                // If we are aborting don't wait for scanners to finish
                return;
            }
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                RegionServerServices regionServerServices = (RegionServerServices)c.getEnvironment().getOnlineRegions();
                boolean shuttingDown = !regionServerServices.isClusterUp() || regionServerServices.isStopping() || regionServerServices.isAborted();
                if (latest.currentScannerCount>0 && !shuttingDown) {
                    SpliceLogUtils.warn(LOG, "preClose Delayed waiting for scanners to complete scannersRemaining=%d",latest.currentScannerCount);
                    try {
                        Thread.sleep(1000); // Have Split sleep for a second
                    } catch (InterruptedException e1) {
                        throw new IOException(e1);
                    }
                } else {
                    if (memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, true)))
                        break;
                }
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        try {
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                if(memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, false)))
                    break;
            }
        } catch (Throwable t) {
            LOG.error("Unexpected exception on close, loggin it", t);
        }
    }


    protected boolean startRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] startRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), startRow);
    }

    protected boolean stopRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] stopRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), stopRow)
                || Bytes.equals(c.getEnvironment().getRegion().getRegionInfo().getEndKey(), stopRow);

    }

    protected long getReadpoint(HRegion region){
        return region.getMVCC().getReadPoint();
    }

    public static  String displayByteArray(byte[] key) {
        if (key==null)
            return "NULL";
        if (key.length == 0)
            return "";
        return Bytes.toHex(key);
    }

    public static void main(String...args) throws Exception {
        long timeWaited = 0l;
        for (int i = 1; i <= 40; i++) {
            timeWaited += ConnectionUtils.getPauseTime(90, i);
        }
        System.out.printf("timeWaited: %d sec%n", timeWaited);
    }

    public MemstoreAware getMemstoreAware() {
        return memstoreAware.get();
    }
}
