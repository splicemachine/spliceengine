/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access.client;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Flush Aware Scanner that handles memstore only scans.
 *
 * The scan needs to send
 *
 * (1) begin message
 * (rowkey=null,timestamp=0l,H,H,H)
 *
 * (2) any records
 * (normal SI Formatted rowkeys)
 *
 * (3) terminate message(s)  It can never return null because that automatically terminates the scan.
 * (rowkey=null,timestamp=MAX_TIMESTAMP,H,H,H)
 *
 * If a flush occurs, it needs to send a flush message
 *
 * (rowkey=null,timestamp=0l,F,F,F)
 * 
 */
public class MemStoreFlushAwareScanner extends StoreScanner {
    protected static final Logger LOG = Logger.getLogger(MemStoreFlushAwareScanner.class);
    protected AtomicReference<MemstoreAware> memstoreAware;
    protected MemstoreAware initialValue;
    protected HRegion region;
    protected boolean beginRow = true;
    protected boolean endRowNeedsToBeReturned = false;
    protected boolean flushAlreadyReturned = false;
    protected int counter = 0;

    public MemStoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan,
                                     final NavigableSet<byte[]> columns, long readPt, AtomicReference<MemstoreAware> memstoreAware, MemstoreAware initialValue) throws IOException {
        super(store, scanInfo, scan, columns, readPt);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "init for region=%s, scan=%s", region.getRegionInfo().getRegionNameAsString(),scan);
        this.memstoreAware = memstoreAware;
        this.initialValue = initialValue;
        this.region = region;
    }

    @Override
    public KeyValue peek() {
        if (didWeFlush()) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "already Flushed");
            if (flushAlreadyReturned) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "returning counter");
                return new KeyValue(Bytes.toBytes(counter),ClientRegionConstants.FLUSH,ClientRegionConstants.FLUSH, 0l,ClientRegionConstants.FLUSH);
            }
            else {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "returning flush");
                return ClientRegionConstants.MEMSTORE_BEGIN_FLUSH;
            }
        }
        if (beginRow)
            return ClientRegionConstants.MEMSTORE_BEGIN;
        Cell peek = super.peek();
        if (peek == null) {
            endRowNeedsToBeReturned = true;
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "endRow -->" + counter);
            return new KeyValue(Bytes.toBytes(counter),ClientRegionConstants.HOLD,ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP,ClientRegionConstants.HOLD);
        }
        return (KeyValue)peek;
    }

    @Override
    public KeyValue next() {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next");
        throw new RuntimeException("Not Implemented");
    }

    //		@Override
    public boolean seek(KeyValue key) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "seek with key=%s",key);
        throw new IOException("Not Implemented");
    }

    //		@Override
    public boolean reseek(KeyValue kv) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "reseek kv=%s",kv);
        throw new IOException("reseek not implemented");
    }

    public boolean internalNext(List<Cell> outResult,ScannerContext scannerContext) throws IOException {
        if (beginRow) {
            beginRow = false;
            return outResult.add(ClientRegionConstants.MEMSTORE_BEGIN);
        }
        if (endRowNeedsToBeReturned) {
            try {
                outResult.add(new KeyValue(Bytes.toBytes(counter), ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD));
                return HBasePlatformUtils.scannerEndReached(scannerContext);
            } finally {
                counter++;
            }
        }
        if (didWeFlush()) {
            if (flushAlreadyReturned) {
                try {
                    outResult.add(new KeyValue(Bytes.toBytes(counter),
                            ClientRegionConstants.FLUSH, ClientRegionConstants.FLUSH, Long.MAX_VALUE, ClientRegionConstants.FLUSH));
                    return HBasePlatformUtils.scannerEndReached(scannerContext);
                } finally {
                    counter++;
                }
            } else {
                flushAlreadyReturned = true;
                outResult.add(ClientRegionConstants.MEMSTORE_BEGIN_FLUSH);
            }
            return HBasePlatformUtils.scannerEndReached(scannerContext);
        }
        return directInternalNext(outResult,scannerContext);
    }

    @Override
    public void close() {
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "close");
        }
        super.close();
        boolean shouldC;
        do{
            MemstoreAware latest = memstoreAware.get();
            shouldC = !memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest));
        } while (shouldC);
    }

    private boolean didWeFlush() {
        return memstoreAware.get().totalFlushCount != initialValue.totalFlushCount;
    }


    @Override
    public boolean next(List<Cell> outResult) throws IOException {
        return internalNext(outResult,NoLimitScannerContext.getInstance());
    }


    boolean directInternalNext(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return super.next(result,scannerContext);
    }

    @Override
    public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
        return internalNext(outResult,scannerContext);
    }

}
