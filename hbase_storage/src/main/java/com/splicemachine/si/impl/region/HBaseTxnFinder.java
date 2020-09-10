package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
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
            return -1L; // in some rare scenarios, we could have a series of ROLLBACKs.
        }

        protected Pair<Long, Long> decode(List<Cell> cells) {
            if (cells.size() <= 0) return null;
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

    @Override
    public Pair<Long, Long> find(byte bucket, byte[] begin, boolean reverse) throws IOException {
        Scan hbaseScan = new Scan();
        hbaseScan.addColumn(V2TxnDecoder.FAMILY, V2TxnDecoder.STATE_QUALIFIER_BYTES)
                .setReversed(reverse)
                .readAllVersions()
                .setFilter(new PrefixFilter(new byte[]{bucket})); // todo get one row only
        if (begin != null) {
            hbaseScan.withStartRow(begin);
        }
        HBaseTxnFinder.ScanTimestampIterator si = new ScanTimestampIterator(region.getScanner(hbaseScan));
        if (si.hasNext()) {
            return si.next;
        } else {
            return null;
        }
    }
}
