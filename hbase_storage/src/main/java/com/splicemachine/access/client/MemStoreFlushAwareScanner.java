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

package com.splicemachine.access.client;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
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
public class MemStoreFlushAwareScanner extends StoreScanner implements RegionScanner {
    protected static final Logger LOG = Logger.getLogger(MemStoreFlushAwareScanner.class);
    protected AtomicReference<MemstoreAware> memstoreAware;
    protected MemstoreAware initialValue;
    protected HRegion region;
    protected boolean beginRow = true;
    protected boolean endRowNeedsToBeReturned = false;
    protected boolean flushAlreadyReturned = false;
    protected int counter = 0;
    private final byte[] stopRow;
    private Filter filter;
    private int batch;

    public MemStoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan,
                                     final NavigableSet<byte[]> columns, long readPt, AtomicReference<MemstoreAware> memstoreAware, MemstoreAware initialValue) throws IOException {
        super((HStore) store, scanInfo, scan, columns, readPt);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "init for region=%s, scan=%s", region.getRegionInfo().getRegionNameAsString(),scan);
        this.memstoreAware = memstoreAware;
        this.initialValue = initialValue;
        this.region = region;
        this.stopRow = Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) ? null : scan.getStopRow();
        this.filter = scan.getFilter();
        this.batch = scan.getBatch();
    }
    
    protected boolean isStopRow(Cell peek) {
        return (stopRow!= null &&
                region.getCellComparator().compareRows(peek, stopRow, 0, stopRow.length) > 0);
    }

    @Override
    public KeyValue peek() {
        if (beginRow) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Peek: memstore begin");
            return ClientRegionConstants.MEMSTORE_BEGIN;
        }
        if (endRowNeedsToBeReturned) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Peek: end row " + counter);
            return new KeyValue(Bytes.toBytes(counter), ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD);
        }
        if (didWeFlush()) {
            if (flushAlreadyReturned) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Peek: flushed " + counter);
                return new KeyValue(Bytes.toBytes(counter),ClientRegionConstants.FLUSH,ClientRegionConstants.FLUSH, 0l,ClientRegionConstants.FLUSH);
            }
            else {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Peek: begin flush");
                return ClientRegionConstants.MEMSTORE_BEGIN_FLUSH;
            }
        }
        Cell peek = super.peek();
        if (peek == null || isStopRow(peek)) {
            endRowNeedsToBeReturned = true;
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Peek: first end row");
            return new KeyValue(Bytes.toBytes(counter),ClientRegionConstants.HOLD,ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP,ClientRegionConstants.HOLD);
        }
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Peek: " + peek);
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
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Next: memstore begin");
            return outResult.add(ClientRegionConstants.MEMSTORE_BEGIN);
        }
        if (endRowNeedsToBeReturned) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Next: end row " + counter);
            try {
                outResult.add(new KeyValue(Bytes.toBytes(counter), ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD));
                return HBasePlatformUtils.scannerEndReached(scannerContext);
            } finally {
                counter++;
            }
        }
        if (didWeFlush()) {
            if (flushAlreadyReturned) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Next: flushed " + counter);
                try {
                    outResult.add(new KeyValue(Bytes.toBytes(counter),
                            ClientRegionConstants.FLUSH, ClientRegionConstants.FLUSH, Long.MAX_VALUE, ClientRegionConstants.FLUSH));
                    return HBasePlatformUtils.scannerEndReached(scannerContext);
                } finally {
                    counter++;
                }
            } else {
                flushAlreadyReturned = true;
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "Next: returning begin flush ");
                outResult.add(ClientRegionConstants.MEMSTORE_BEGIN_FLUSH);
            }
            return HBasePlatformUtils.scannerEndReached(scannerContext);
        }
        if (super.next(outResult,scannerContext)) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Next: returning " + outResult.size());
            return true;
        }

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Next: returning first end row ");
        // We don't have more rows but can't return null here
        endRowNeedsToBeReturned = true;
        if (outResult.isEmpty()) {
            outResult.add(new KeyValue(Bytes.toBytes(counter), ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD));
        }
        return HBasePlatformUtils.scannerEndReached(scannerContext);
    }

    @Override
    public void close() {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "close");
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

    @Override
    public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
        return internalNext(outResult,scannerContext);
    }


    @Override
    public long getMvccReadPoint() {
        return region.getMVCC().getReadPoint();
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return next(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result, scannerContext);
    }

    @Override
    public int getBatch() {
        return batch;
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return filter!=null && filter.filterAllRemaining();
    }

    @Override
    public long getMaxResultSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "reseek kv=%s",Bytes.toHex(row));
        throw new IOException("reseek not implemented");
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }
}
