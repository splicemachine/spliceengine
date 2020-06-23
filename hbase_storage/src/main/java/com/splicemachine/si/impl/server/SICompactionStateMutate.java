package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.sun.istack.NotNull;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;


class SICompactionStateMutate {
    private static class CellAndCommit implements Comparable{
        private Cell cell;
        private Long commitTimestamp;

        CellAndCommit(Cell cell, Long commitTimestamp) {
            this.cell = cell;
            this.commitTimestamp = commitTimestamp;
        }

        Cell getCell() {
            return cell;
        }

        Long getCommitTimestamp() {
            return commitTimestamp;
        }

        @Override
        public int compareTo(@NotNull Object other) {
            return KeyValue.COMPARATOR.compare(cell, ((CellAndCommit) other).cell);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cell, commitTimestamp);
        }

        @Override
        public boolean equals(@NotNull Object other) {
            if (!(other instanceof CellAndCommit)) {
                return false;
            }
            return compareTo(other) == 0;
        }
    }


    private static final Logger LOG = Logger.getLogger(SICompactionStateMutate.class);
    private SortedSet<CellAndCommit> dataToReturn = new TreeSet<>();
    private final PurgeConfig purgeConfig;
    private long maxTombstoneTimestamp = 0;
    private long lowWatermarkTransaction;
    private boolean firstWriteToken = false;
    private long deleteRightAfterFirstWriteTimestamp = 0;
    private Map<Integer, Long> columnUpdateLatestTimestamp = new HashMap<>();
    private Set<Long> updatesToPurgeTimestamps = new HashSet<>();

    SICompactionStateMutate(PurgeConfig purgeConfig, long lowWatermarkTransaction) {
        this.purgeConfig = purgeConfig;
        this.lowWatermarkTransaction = lowWatermarkTransaction;
    }

    private boolean isSorted(List<Cell> list) {
        if (list.size() <= 1) {
            return true;
        }
        Iterator<Cell> iter = list.iterator();
        Cell previous = iter.next();
        while(iter.hasNext()) {
            Cell next = iter.next();
            if (KeyValue.COMPARATOR.compare(next, previous) < 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }

    public void mutate(List<Cell> rawList, List<TxnView> txns, List<Cell> results) throws IOException {
        assert dataToReturn.isEmpty();
        assert results.isEmpty();
        assert maxTombstoneTimestamp == 0;
        //assert isSorted(rawList): "CompactionStateMutate: rawList not sorted";
        assert rawList.size() == txns.size();

        try {
            Iterator<TxnView> it = txns.iterator();
            for (Cell aRawList : rawList) {
                TxnView txn = it.next();
                mutate(aRawList, txn);
            }
            Stream<CellAndCommit> stream = dataToReturn.stream();
            if (purgeConfig.shouldPurgeDeletes() &&
                    (!purgeConfig.shouldRespectActiveTransactions() || maxTombstoneTimestamp > 0)) {
                stream = stream.filter(not(this::purgeableDeletedRow));
            }
            if (purgeConfig.shouldPurgeUpdates() && !updatesToPurgeTimestamps.isEmpty()) {
                stream = stream.filter(not(this::purgeableOldUpdate));
            }
            stream.map(CellAndCommit::getCell).forEachOrdered(results::add);
            //assert isSorted(results) : "CompactionStateMutate: results not sorted";
        } catch (AssertionError e) {
            LOG.error(e);
            LOG.error(rawList.toString());
            LOG.error(txns.toString());
            throw e;
        }
    }

    /**
     * Apply SI mutation logic to an individual key-value.
     */
    private void mutate(Cell element, TxnView txn) throws IOException {
        final CellType cellType = CellUtils.getKeyValueType(element);
        if (element.getType() != Cell.Type.Put) {
            // Rolled back data, remote it
            return;
        }
        if (cellType == CellType.COMMIT_TIMESTAMP) {
            assert txn == null;
            assert element.getValueLength() == 8: "Element does not contain a timestamp: " + element;
            long commitTimestamp = Bytes.toLong(element.getValueArray(), element.getValueOffset(), element.getValueLength());
            dataToReturn.add(new CellAndCommit(element, commitTimestamp));
            return;
        }
        if (txn == null) {
            // we don't have transactional information, just return the data as is
            dataToReturn.add(new CellAndCommit(element, null));
            return;
        }
        //assert element.getTimestamp() == txn.getBeginTimestamp();
        Txn.State txnState = txn.getEffectiveState();

        if (txnState == Txn.State.ROLLEDBACK) {
            // rolled back data, remove it from the compacted data
            return;
        }

        if (txnState == Txn.State.ACTIVE) {
            dataToReturn.add(new CellAndCommit(element, null));
            return;
        }

        assert txnState == Txn.State.COMMITTED;

        /*
         * This element has been committed all the way to the user level, so a
         * commit timestamp can be placed on it.
         */
        long commitTimestamp = txn.getEffectiveCommitTimestamp();
        dataToReturn.add(new CellAndCommit(
                newTransactionTimeStampKeyValue(element, Bytes.toBytes(commitTimestamp)), commitTimestamp));
        switch (cellType) {
            case TOMBSTONE:
                if (commitTimestamp > maxTombstoneTimestamp &&
                        (!purgeConfig.shouldRespectActiveTransactions() || commitTimestamp < lowWatermarkTransaction)) {
                    maxTombstoneTimestamp = commitTimestamp;
                }
                break;
            case FIRST_WRITE_TOKEN:
                assert !firstWriteToken;
                firstWriteToken = true;
                break;
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                assert deleteRightAfterFirstWriteTimestamp == 0;
                deleteRightAfterFirstWriteTimestamp = commitTimestamp;
                break;
            case USER_DATA: {
                if (purgeConfig.shouldPurgeUpdates() && commitTimestamp < lowWatermarkTransaction) {
                    EntryDecoder decoder = new EntryDecoder(element.getValueArray(), element.getValueOffset(), element.getValueLength());
                    BitIndex index = decoder.getCurrentIndex();
                    LOG.trace("BitIndex: " + index + " , length=" + index.length());
                    boolean purge = true;
                    for (int col = index.nextSetBit(0); col >= 0; col = index.nextSetBit(col + 1)) {
                        if (!columnUpdateLatestTimestamp.containsKey(col)) {
                            columnUpdateLatestTimestamp.put(col, commitTimestamp);
                            purge = false;
                            LOG.trace("Update cannot be purged: " + element);
                        } else {
                            assert commitTimestamp <= columnUpdateLatestTimestamp.get(col);
                        }
                    }
                    if (purge) {
                        boolean ret = updatesToPurgeTimestamps.add(commitTimestamp);
                        assert ret;
                    }
                }
                break;
            }
            default:
                break;
        }
        dataToReturn.add(new CellAndCommit(element, commitTimestamp));
    }

    private boolean shouldRemoveMostRecentTombstone() {
        switch (purgeConfig.getPurgeLatestTombstone()) {
            case ALWAYS:
                return true;
            case IF_DELETE_FOLLOWS_FIRST_WRITE:
                return firstWriteToken && deleteRightAfterFirstWriteTimestamp == maxTombstoneTimestamp;
            case IF_FIRST_WRITE_PRESENT:
                return firstWriteToken;
        }
        assert false;
        return false;
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    private boolean purgeableDeletedRow(CellAndCommit element) {
        Long timestamp = element.getCommitTimestamp();
        if (timestamp == null)
            return false;
        if (timestamp == maxTombstoneTimestamp && shouldRemoveMostRecentTombstone())
            return true;
        return timestamp < maxTombstoneTimestamp;
    }

    private boolean purgeableOldUpdate(CellAndCommit element) {
        Long timestamp = element.getCommitTimestamp();
        return timestamp != null && updatesToPurgeTimestamps.contains(timestamp);
    }

    private static Cell newTransactionTimeStampKeyValue(Cell element, byte[] value) {
        return new KeyValue(element.getRowArray(),
                element.getRowOffset(),
                element.getRowLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,0,1,
                SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,
                element.getTimestamp(),KeyValue.Type.Put,
                value,0,value==null?0:value.length);
    }
}
