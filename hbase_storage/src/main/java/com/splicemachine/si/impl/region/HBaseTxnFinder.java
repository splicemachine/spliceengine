package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class HBaseTxnFinder implements TxnFinder {

    private HRegion region;

    public HBaseTxnFinder(HRegion region) {
        this.region = region;
    }

    private static class ScanTimestampIterator implements Source<Pair<Long, Long>> {
        private final RegionScanner regionScanner;
        protected Pair<Long, Long> next;
        private List<Cell> currentResults;

        /**
         * Returns the timestamp of the cell whose state is {@link Txn.State.ACTIVE}.
         * @param stateVersions list of kvs each representing a version of txn state.
         * @return The timestamp of the cell whose state ACTIVE, otherwise -1.
         */
        private long getTimestampOfActiveState(List<Cell> stateVersions) {
            for(Cell stateKv : stateVersions) {
                Txn.State state = Txn.State.decode(stateKv.getValueArray(),stateKv.getValueOffset(),stateKv.getValueLength());
                if(state == Txn.State.ACTIVE) {
                    return stateKv.getTimestamp();
                }
            }
            return -1L; // should never happen.
        }

        protected Pair<Long, Long> decode(List<Cell> cells) {
            long ts = 0;
                ts = getTimestampOfActiveState(cells);
                if(ts == -1) {
                    return null;
                }
            long txnId = TxnUtils.txnIdFromRowKey(cells.get(0).getRowArray(), cells.get(0).getRowOffset(), cells.get(0).getRowLength());
            return new Pair<>(txnId, ts);
        }

        public ScanTimestampIterator(RegionScanner scanner) {
            this.regionScanner = scanner;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (next != null) return true;
            if (currentResults == null)
                currentResults = new ArrayList<>(10);
            boolean shouldContinue;
            do {
                shouldContinue = regionScanner.next(currentResults); // get next row.
                if (currentResults.size() <= 0) return false;

                this.next = decode(currentResults);
                currentResults.clear();
            } while (next == null && shouldContinue);

            return next != null;
        }

        @Override
        public Pair<Long, Long> next() throws IOException {
            if (!hasNext()) throw new NoSuchElementException();
            Pair<Long, Long> n = next;
            next = null;
            return n;
        }

        @Override
        public void close() throws IOException {
            regionScanner.close();
        }
    }

    private static class TxWithTimestampFilter extends FilterBase {

        /**
         * always return false so we make sure that {@link #filterCell(Cell)} method is called.
         */
        @Override
        public boolean filterRowKey(Cell cell) {
            return false;
        }

        @Override
        public ReturnCode filterCell(final Cell cell) throws IOException {
            Txn.State state = Txn.State.decode(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            if(state == Txn.State.ACTIVE) {
                return ReturnCode.INCLUDE;
            } else {
                return ReturnCode.SKIP;
            }
        }
    }

    /**
     * <p>When scanning for a prefix the scan should stop immediately after the the last row that
     * has the specified prefix. This method calculates the closest next rowKey immediately following
     * the given rowKeyPrefix.</p>
     * <p><b>IMPORTANT: This converts a rowKey<u>Prefix</u> into a rowKey</b>.</p>
     * <p>If the prefix is an 'ASCII' string put into a byte[] then this is easy because you can
     * simply increment the last byte of the array.
     * But if your application uses real binary rowids you may run into the scenario that your
     * prefix is something like:</p>
     * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x23, 0xFF, 0xFF }</b><br/>
     * Then this stopRow needs to be fed into the actual scan<br/>
     * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x24 }</b> (Notice that it is shorter now)<br/>
     * This method calculates the correct stop row value for this usecase.
     *
     * @param rowKeyPrefix the rowKey<u>Prefix</u>.
     * @return the closest next rowKey immediately following the given rowKeyPrefix.
     */
    private byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
        // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
        // Search for the place where the trailing 0xFFs start
        int offset = rowKeyPrefix.length;
        while (offset > 0) {
            if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                break;
            }
            offset--;
        }

        if (offset == 0) {
            // We got an 0xFFFF... (only FFs) stopRow value which is
            // the last possible prefix before the end of the table.
            // So set it to stop at the 'end of the table'
            return HConstants.EMPTY_END_ROW;
        }

        // Copy the right length of the original
        byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
        // And increment the last one
        newStopRow[newStopRow.length - 1]++;
        return newStopRow;
    }

    @Override
    public Pair<Long, Long> find(byte bucket, byte[] begin, boolean reverse) throws IOException {
        Scan hbaseScan = new Scan();

        FilterList filterList = new FilterList();
        filterList.addFilter(new PrefixFilter(new byte[]{bucket}));
        filterList.addFilter(new TxWithTimestampFilter());

        hbaseScan.addColumn(V2TxnDecoder.FAMILY, V2TxnDecoder.STATE_QUALIFIER_BYTES)
                .setReversed(reverse)
                .readAllVersions()
                .setLimit(1)
                .setFilter(filterList);

        if (begin != null) {
            hbaseScan.withStartRow(begin);
        } else {
            if(reverse)
                hbaseScan.withStartRow(calculateTheClosestNextRowKeyForPrefix(new byte[]{bucket}));
            else
                hbaseScan.withStartRow(new byte[]{bucket});
        }
        HBaseTxnFinder.ScanTimestampIterator si = new ScanTimestampIterator(region.getScanner(hbaseScan));
        if (si.hasNext()) {
            return si.next;
        } else {
            return null;
        }
    }
}
