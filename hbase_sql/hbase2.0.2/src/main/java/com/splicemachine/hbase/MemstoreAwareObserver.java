/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
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
public class MemstoreAwareObserver extends BaseMemstoreAwareObserver implements RegionCoprocessor{
    private static final Logger LOG = Logger.getLogger(MemstoreAwareObserver.class);

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {
        return preCompactAction(c, store, scanner, scanType, request);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile,
                            CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        postCompactAction(c, store, resultFile, request);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner,
                                    FlushLifeCycleTracker tracker) throws IOException {
        return preFlushAction(c, store, scanner);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile,
                          FlushLifeCycleTracker tracker) throws IOException{
        postFlushAction(c, store, resultFile);
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store, ScanOptions options) throws IOException {

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
            return preStoreScannerOpenAction2(c, store, scan, null, s);
        }
        return s;
    }

    protected RegionScanner preStoreScannerOpenAction2(ObserverContext<RegionCoprocessorEnvironment> c,Store store,Scan scan,NavigableSet<byte[]> targetCols, RegionScanner s) throws IOException{
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
                        SpliceLogUtils.warn(LOG, "splitting, merging, or active compaction on scan on %s : %s", c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString(), currentState);
                        throw new IOException("splitting, merging, or active compaction on scan on " + c.getEnvironment().getRegion().getRegionInfo().getRegionNameAsString());
                    }

                    if (memstoreAware.compareAndSet(currentState, MemstoreAware.incrementScannerCount(currentState)))
                        break;
                }
                if (Bytes.equals(startKey,c.getEnvironment().getRegionInfo().getStartKey()) &&
                        Bytes.equals(endKey,c.getEnvironment().getRegionInfo().getEndKey()) &&
                        Bytes.equals(serverName,Bytes.toBytes(HBasePlatformUtils.getRegionServerServices(c.getEnvironment()).getServerName().getHostAndPort()))
                        ) {
                    // Partition Hit
                    InternalScan iscan = new InternalScan(scan);
                    iscan.checkOnlyMemStore();
                    HRegion region = (HRegion) c.getEnvironment().getRegion();
                    return new MemStoreFlushAwareScanner(region, store, ((HStore)store).getScanInfo(), iscan, targetCols, getReadpoint(region), memstoreAware, memstoreAware.get());
                } else { // Partition Miss
                    while (true) {
                        MemstoreAware latest = memstoreAware.get();
                        if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest)))
                            break;
                    }
                    SpliceLogUtils.warn(LOG, "scan missed do to split after task creation " +
                                    "scan [%s,%s], partition[%s,%s], region=[%s,%s]," +
                                    "server=[%s,%s]",
                            displayByteArray(scan.getStartRow()),
                            displayByteArray(scan.getStopRow()),
                            displayByteArray(startKey),
                            displayByteArray(endKey),
                            displayByteArray(c.getEnvironment().getRegionInfo().getStartKey()),
                            displayByteArray(c.getEnvironment().getRegionInfo().getEndKey()),
                            Bytes.toString(serverName),
                            HBasePlatformUtils.getRegionServerServices(c.getEnvironment()).getServerName().getHostAndPort()
                    );

                    throw new DoNotRetryIOException();
                }
            }else return s;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }

    }

}
