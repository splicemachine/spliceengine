package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;


class SICompactionStateMutate {
    /**
     * We consider the value length so that a tombstone and an anti tombstone
     * with the same timestamp, same sequence id don't evaluate equally.
     */
    private static class CellComparatorWithValueLength implements Comparator<Cell> {
        private Comparator<Cell> keyComparator = KeyValue.COMPARATOR;

        @Override
        public int compare(Cell o1, Cell o2) {
            int keyComp = keyComparator.compare(o1, o2);
            if (keyComp != 0)
                return keyComp;
            return Integer.compare(o2.getValueLength(), o1.getValueLength());
        }
    }

    private static final Logger LOG = Logger.getLogger(SICompactionStateMutate.class);
    private SortedSet<Cell> dataToReturn = new TreeSet<>(new CellComparatorWithValueLength());
    private final PurgeConfig purgeConfig;
    private Cell maxTombstone = null;
    private Cell lastSeenAntiTombstone = null;
    private long lowWatermarkTransaction;
    private boolean firstWriteToken = false;
    private long deleteRightAfterFirstWriteTimestamp = 0;
    private Map<Integer, Long> columnUpdateLatestTimestamp = new HashMap<>();
    private Set<Long> updatesToPurgeTimestamps = new HashSet<>();
    private boolean firstUpdateCell = true;

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
        assert maxTombstone == null;
        //assert isSorted(rawList): "CompactionStateMutate: rawList not sorted";
        assert rawList.size() == txns.size();

        try {
            Iterator<TxnView> it = txns.iterator();
            for (Cell aRawList : rawList) {
                TxnView txn = it.next();
                mutate(aRawList, txn);
            }
            Stream<Cell> stream = dataToReturn.stream();
            if (shouldPurgeDeletes())
                stream = stream.filter(not(this::purgeableDeletedRow));
            if (shouldPurgeUpdates())
                stream = stream.filter(not(this::purgeableOldUpdate));
            stream.forEachOrdered(results::add);
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
        if (element.getTypeByte() != KeyValue.Type.Put.getCode()) {
            // Rolled back data, remove it
            return;
        }
        if (cellType == CellType.COMMIT_TIMESTAMP) {
            assert txn == null;
            assert element.getValueLength() == 8: "Element does not contain a timestamp: " + element;
            dataToReturn.add(element);
            return;
        }
        if (txn == null) {
            // we don't have transactional information, just return the data as is
            dataToReturn.add(element);
            return;
        }
        Txn.State txnState = txn.getEffectiveState();

        if (txnState == Txn.State.ROLLEDBACK) {
            // rolled back data, remove it from the compacted data
            return;
        }

        if (txnState == Txn.State.ACTIVE) {
            dataToReturn.add(element);
            return;
        }

        assert txnState == Txn.State.COMMITTED;

        /*
         * This element has been committed all the way to the user level, so a
         * commit timestamp can be placed on it.
         */
        long commitTimestamp = txn.getEffectiveCommitTimestamp();
        long beginTimestamp = element.getTimestamp();
        dataToReturn.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(commitTimestamp)));
        switch (cellType) {
            case TOMBSTONE:
                assert maxTombstone == null || maxTombstone.getTimestamp() >= beginTimestamp;
                if (maxTombstone == null &&
                        (!purgeConfig.shouldRespectActiveTransactions() || commitTimestamp < lowWatermarkTransaction)) {
                    if (lastSeenAntiTombstone != null && lastSeenAntiTombstone.getTimestamp() == beginTimestamp) {
                        maxTombstone = lastSeenAntiTombstone;
                    } else {
                        maxTombstone = element;
                    }
                }
                break;
            case ANTI_TOMBSTONE:
                // Anti tombstones are only relevant if they share a timestamp with a tombstone.
                // In that case, we cannot purge elements of that timestamp because we might purge
                // elements that were inserted after the tombstone
                if (maxTombstone != null && maxTombstone.getTimestamp() == beginTimestamp) {
                    maxTombstone = element;
                }
                lastSeenAntiTombstone = element;
                break;
            case FIRST_WRITE_TOKEN:
                assert !firstWriteToken;
                firstWriteToken = true;
                break;
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                assert deleteRightAfterFirstWriteTimestamp == 0;
                deleteRightAfterFirstWriteTimestamp = beginTimestamp;
                break;
            case USER_DATA: {
                if (purgeConfig.shouldPurgeUpdates() && commitTimestamp < lowWatermarkTransaction) {
                    EntryDecoder decoder = new EntryDecoder(element.getValueArray(), element.getValueOffset(), element.getValueLength());
                    BitIndex index = decoder.getCurrentIndex();
                    LOG.trace("BitIndex: " + index + " , length=" + index.length());
                    boolean purge = !firstUpdateCell;
                    firstUpdateCell = false;
                    for (int col = index.nextSetBit(0); col >= 0; col = index.nextSetBit(col + 1)) {
                        if (!columnUpdateLatestTimestamp.containsKey(col)) {
                            columnUpdateLatestTimestamp.put(col, beginTimestamp);
                            purge = false;
                            LOG.trace("Update cannot be purged: " + element);
                        } else {
                            assert beginTimestamp < columnUpdateLatestTimestamp.get(col);
                        }
                    }
                    if (purge) {
                        boolean ret = updatesToPurgeTimestamps.add(beginTimestamp);
                        assert ret;
                    }
                }
                break;
            }
            default:
                break;
        }
        dataToReturn.add(element);
    }

    private boolean shouldPurgeDeletes() {
        return purgeConfig.shouldPurgeDeletes() && maxTombstone != null;
    }

    private boolean shouldPurgeUpdates() {
        return purgeConfig.shouldPurgeUpdates() && !updatesToPurgeTimestamps.isEmpty();
    }

    private boolean shouldRemoveMostRecentTombstone() {
        assert maxTombstone != null;
        switch (purgeConfig.getPurgeLatestTombstone()) {
            case ALWAYS:
                return true;
            case IF_DELETE_FOLLOWS_FIRST_WRITE:
                return firstWriteToken && deleteRightAfterFirstWriteTimestamp == maxTombstone.getTimestamp();
            case IF_FIRST_WRITE_PRESENT:
                return firstWriteToken;
        }
        assert false;
        return false;
    }

    public static <R> Predicate<R> not(Predicate<R> predicate) {
        return predicate.negate();
    }

    private boolean purgeableDeletedRow(Cell element) {
        if (maxTombstone == null) {
            return false;
        }
        if (element.getTimestamp() == maxTombstone.getTimestamp() &&
                CellUtils.getKeyValueType(maxTombstone) == CellType.TOMBSTONE &&
                shouldRemoveMostRecentTombstone())
            return true;
        return element.getTimestamp() < maxTombstone.getTimestamp();
    }

    private boolean purgeableOldUpdate(Cell element) {
        return updatesToPurgeTimestamps.contains(element.getTimestamp());
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
