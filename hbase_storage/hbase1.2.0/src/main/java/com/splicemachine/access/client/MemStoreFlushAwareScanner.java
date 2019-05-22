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

package com.splicemachine.access.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.NoLimitScannerContext;


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
    protected boolean endRowWasReturned = false;
    protected boolean flushAlreadyReturned = false;
    private final byte[] stopRow;
    protected int counter = 0;
    protected boolean hasMultiRowRangeFilter;
    protected byte[] lastRowkey;

    public MemStoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan,
                                     final NavigableSet<byte[]> columns, long readPt, AtomicReference<MemstoreAware> memstoreAware, MemstoreAware initialValue) throws IOException {
        super(store, scanInfo, scan, columns, readPt);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "init for region=%s, scan=%s", region.getRegionInfo().getRegionNameAsString(),scan);
        this.memstoreAware = memstoreAware;
        this.initialValue = initialValue;
        this.region = region;
        this.stopRow = Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW) ? null : scan.getStopRow();
        if (this.scan.hasFilter() && this.scan.getFilter() instanceof MultiRowRangeFilter) {
            hasMultiRowRangeFilter = true;
        }
        else
            hasMultiRowRangeFilter = false;
    }

    private byte[] getFakeRowkey() {
        byte[] counterBytes = Bytes.toBytes(counter);
        if (!hasMultiRowRangeFilter || lastRowkey == null)
            return counterBytes;
        else
            // Use a non-filtered rowkey higher than all others.
            return com.google.common.primitives.Bytes.concat(lastRowkey, counterBytes);
    }

    private void updateLatestRow(Cell lastCell) {
        byte [] currentRow = lastCell.getRowArray();
        int offset         = lastCell.getRowOffset();
        short length       = lastCell.getRowLength();

        if (lastRowkey == null || lastRowkey.length < length)
            lastRowkey = new byte [length];

        System.arraycopy(currentRow, offset, lastRowkey, 0, length);
        Arrays.fill(lastRowkey, (int)length, lastRowkey.length, (byte)0);
    }
    
    protected boolean isStopRow(Cell peek) {
        byte[] currentRow = peek.getRowArray();
        int offset = peek.getRowOffset();
        short length = peek.getRowLength();
        return (stopRow!= null &&
                region.getComparator().compareRows(stopRow, 0, stopRow.length,
                        currentRow, offset, length) <= 0);
    }

    @Override
    public KeyValue peek() {
        if (didWeFlush()) {
            if (flushAlreadyReturned) {
                byte[] rowKey = getFakeRowkey();
                return new KeyValue(rowKey,ClientRegionConstants.FLUSH,ClientRegionConstants.FLUSH, 0l,ClientRegionConstants.FLUSH);
            }
            else {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "returning flush");
                return ClientRegionConstants.MEMSTORE_BEGIN_FLUSH;
            }
        }
        if (beginRow) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Peek: memstore begin");
            return ClientRegionConstants.MEMSTORE_BEGIN;
        }
        Cell peek = super.peek();
        if (peek == null || isStopRow(peek)) {
            endRowNeedsToBeReturned = true;
            byte[] rowKey = getFakeRowkey();
            return new KeyValue(rowKey,ClientRegionConstants.HOLD,ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP,ClientRegionConstants.HOLD);
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
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Next: memstore begin");
            return outResult.add(ClientRegionConstants.MEMSTORE_BEGIN);
        }
        if (endRowNeedsToBeReturned) {
            try {
                byte[] rowKey = getFakeRowkey();
                outResult.add(new KeyValue(rowKey, ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD));
                endRowWasReturned = true;
                return HBasePlatformUtils.scannerEndReached(scannerContext);
            } finally {
                counter++;
            }
        }
        if (didWeFlush()) {
            if (flushAlreadyReturned) {
                try {
                    byte[] rowKey = getFakeRowkey();
                    outResult.add(new KeyValue(rowKey,
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
        if (directInternalNext(outResult,scannerContext))
            return true;

        // We don't have more rows but can't return null here
        endRowNeedsToBeReturned = true;
        if (outResult.isEmpty()) {
            byte[] rowKey = getFakeRowkey();
            outResult.add(new KeyValue(rowKey, ClientRegionConstants.HOLD, ClientRegionConstants.HOLD, HConstants.LATEST_TIMESTAMP, ClientRegionConstants.HOLD));
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

    boolean directInternalNext(List<Cell> result, ScannerContext scannerContext) throws IOException {
        boolean cellFound = super.next(result,scannerContext);
        if (cellFound && result.size() > 0) {
            updateLatestRow(result.get(result.size()-1));
        }
        return cellFound;
    }

    @Override
    public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
        return internalNext(outResult,scannerContext);
    }


}
