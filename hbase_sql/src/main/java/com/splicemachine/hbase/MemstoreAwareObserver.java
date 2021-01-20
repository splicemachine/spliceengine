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
import com.splicemachine.derby.hbase.*;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.utils.BlockingProbe;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
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
            HRegion region = (HRegion) e.getEnvironment().getRegion();
            scr.setRegion(region);
            return scanner;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile,CompactionRequest request) throws IOException{
        try {
            BlockingProbe.blockPostCompact();
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,byte[] splitRow) throws IOException{
        try {
            BlockingProbe.blockPreSplit();
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                if (latest.currentScannerCount>0) {
                    SpliceLogUtils.warn(LOG, "preSplit Delayed waiting for scanners to complete scannersRemaining=%d",latest.currentScannerCount);
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
    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException{
        try {
            BlockingProbe.blockPostSplit();
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                if(memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, false)))
                    break;
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,InternalScanner scanner) throws IOException{
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
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile) throws IOException{
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
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) throws IOException {
        try {
            if (abortRequested) {
                // If we are aborting don't wait for scanners to finish
                super.preClose(c, abortRequested);
                return;
            }
            while (true) {
                MemstoreAware latest = memstoreAware.get();
                boolean shuttingDown = c.getEnvironment().getRegionServerServices().isStopping() || c.getEnvironment().getRegionServerServices().isAborted();
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
            super.preClose(c, abortRequested);
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
            super.postClose(e, abortRequested);
        } catch (Throwable t) {
            LOG.error("Unexpected exception on close, loggin it", t);
        }
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,Store store,Scan scan,NavigableSet<byte[]> targetCols,KeyValueScanner s) throws IOException{
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
                                " served by " + c.getEnvironment().getRegionServerServices().getServerName() +
                                " with state" + currentState;
                        SpliceLogUtils.warn(LOG, message);
                        throw new IOException(message);
                    }

                    if (memstoreAware.compareAndSet(currentState, MemstoreAware.incrementScannerCount(currentState)))
                        break;
                }
                if (Bytes.equals(startKey,c.getEnvironment().getRegionInfo().getStartKey()) &&
                    Bytes.equals(endKey,c.getEnvironment().getRegionInfo().getEndKey()) &&
                    Bytes.equals(serverName,Bytes.toBytes(c.getEnvironment().getRegionServerServices().getServerName().getHostAndPort()))
                    ) {
                    // Partition Hit
                    InternalScan iscan = new InternalScan(scan);
                    iscan.checkOnlyMemStore();
                    HRegion region = (HRegion) c.getEnvironment().getRegion();
                    // We substitute the pre-created scanner, must close it to avoid a memory leak
                    if (s != null)
                        s.close();
                    return new MemStoreFlushAwareScanner(region, store, store.getScanInfo(), iscan, targetCols, getReadpoint(region), memstoreAware, memstoreAware.get());
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
                            c.getEnvironment().getRegionServerServices().getServerName().getHostAndPort());
                    SpliceLogUtils.warn(LOG, message);

                    throw new DoNotRetryIOException(message);
                }
            }else return s;
        } catch (Throwable t) {
            if (s != null)
                s.close();
            throw CoprocessorUtils.getIOException(t);
        }

    }

    private boolean startRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] startRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), startRow);
    }

    private boolean stopRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] stopRow) {
        return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), stopRow)
                || Bytes.equals(c.getEnvironment().getRegion().getRegionInfo().getEndKey(), stopRow);

    }

    private long getReadpoint(HRegion region){
        return HBasePlatformUtils.getReadpoint(region);
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

    MemstoreAware getMemstoreAware() {
        // for testing only!
        return memstoreAware.get();
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        try {
            super.start(e);
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"starting [%s]",((RegionCoprocessorEnvironment) e).getRegion().getRegionInfo().getRegionNameAsString());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        try {
            super.stop(e);
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"stopping [%s]", ((RegionCoprocessorEnvironment) e).getRegion().getRegionInfo().getRegionNameAsString());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }
}
