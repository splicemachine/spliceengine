package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
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
        private Comparator<Cell> keyComparator = CellComparator.getInstance();

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
    private boolean firstWriteToken = false;
    private long deleteRightAfterFirstWriteTimestamp = 0;
    private boolean bypassPurge = false;
    private Map<Integer, Long> columnUpdateLatestTimestamp = new HashMap<>();
    private Set<Long> updatesToPurgeTimestamps = new HashSet<>();
    private boolean firstUpdateCell = true;

    SICompactionStateMutate(PurgeConfig purgeConfig) {
        this.purgeConfig = purgeConfig;
    }

    private boolean isSorted(List<Cell> list) {
        if (list.size() <= 1) {
            return true;
        }
        Iterator<Cell> iter = list.iterator();
        Cell previous = iter.next();
        while(iter.hasNext()) {
            Cell next = iter.next();
            if (CellComparator.getInstance().compare(next, previous) < 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }

    private void handleSanityChecks(List<Cell> results,
                                    List<Cell> rawList,
                                    List<TxnView> txns) {
        final boolean dataToReturnIsEmpty = dataToReturn.isEmpty();
        final boolean resultsIsEmpty = results.isEmpty();
        final boolean maxTombstoneIsNull = maxTombstone == null;
        final boolean rawListAndTxnListSameSize = rawList.size() == txns.size();
        final boolean debugSortCheck = !LOG.isDebugEnabled() || isSorted(rawList);

        if (!debugSortCheck)
            setBypassPurgeWithWarning("CompactionStateMutate: rawList is not sorted.");
        if (!dataToReturnIsEmpty)
            setBypassPurgeWithWarning("dataToReturn is not properly initialized.");
        if (!resultsIsEmpty)
            setBypassPurgeWithWarning("results list not properly initialized.");
        if (!maxTombstoneIsNull)
            setBypassPurgeWithWarning("maxTombstone not properly initialized to null.");
        if (!rawListAndTxnListSameSize)
            setBypassPurgeWithWarning("rawList and txn list not the same length.");

        assert dataToReturnIsEmpty;
        assert resultsIsEmpty;
        assert maxTombstoneIsNull;
        assert debugSortCheck : "CompactionStateMutate: rawList not sorted";
        assert rawListAndTxnListSameSize;
    }

    private void setBypassPurgeWithWarning(String warningMessage) {
        bypassPurge = true;
        LOG.warn("Skipping tombstone purge.  " + warningMessage);
    }

    /***
     * @return the size of all cells in the `rawList` parameter.
     */
    public long mutate(List<Cell> rawList, List<TxnView> txns, List<Cell> results) throws IOException {
        handleSanityChecks(results, rawList, txns);
        long totalSize = 0;
        try {
            Iterator<TxnView> it = txns.iterator();
            for (Cell cell : rawList) {
                totalSize += KeyValueUtil.length(cell);
                TxnView txn = it.next();
                mutate(cell, txn);
            }
            Stream<Cell> stream = dataToReturn.stream();
            if (shouldPurgeDeletes())
                stream = stream.filter(not(this::purgeableDeletedRow));
            if (shouldPurgeUpdates())
                stream = stream.filter(not(this::purgeableOldUpdate));
            stream.forEachOrdered(results::add);
            final boolean debugSortCheck = !LOG.isDebugEnabled() || isSorted(results);
            if (!debugSortCheck)
                setBypassPurgeWithWarning("CompactionStateMutate: results not sorted.");
            assert debugSortCheck : "CompactionStateMutate: results not sorted";
            return totalSize;
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
            // Rolled back data, remove it
            return;
        }
        if (cellType == CellType.COMMIT_TIMESTAMP) {
            final boolean txnIsNull = txn == null;
            final boolean timeStampInElement = element.getValueLength() == 8;
            if (!txnIsNull)
                setBypassPurgeWithWarning("txn is not null, txn = " + txn.toString());
            if (!timeStampInElement)
                setBypassPurgeWithWarning("Element does not contain a timestamp: " + element);
            assert txnIsNull;
            assert timeStampInElement: "Element does not contain a timestamp: " + element;
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
        if (txnState != Txn.State.COMMITTED)
            setBypassPurgeWithWarning("Attempting to purge an uncommitted transaction, txn = " + txn.toString());
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
                boolean maxTombstoneIsNullOrValid = maxTombstone == null || maxTombstone.getTimestamp() >= beginTimestamp;
                if (!maxTombstoneIsNullOrValid)
                    setBypassPurgeWithWarning("maxTombstone is less than beginTimestamp.  maxTombstone = " + maxTombstone + ", beginTimestamp = " + beginTimestamp);
                assert maxTombstoneIsNullOrValid;
                if (maxTombstone == null &&
                        (!purgeConfig.shouldRespectActiveTransactions() || commitTimestamp < purgeConfig.getTransactionLowWatermark())) {
                    if (lastSeenAntiTombstone != null && lastSeenAntiTombstone.getTimestamp() == beginTimestamp) {
                        maxTombstone = lastSeenAntiTombstone;
                    } else {
                        maxTombstone = element;
                    }
                    maxTombstoneIsNullOrValid = maxTombstone == null || maxTombstone.getTimestamp() >= beginTimestamp;
                    if (!maxTombstoneIsNullOrValid)
                        setBypassPurgeWithWarning("maxTombstone is less than beginTimestamp.  maxTombstone = " + maxTombstone + ", beginTimestamp = " + beginTimestamp);
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
                // Assertions are only thrown on standalone.
                // On clusters, we want to bypass the purge to avoid
                // possible corruption, but also not error out, to
                // prevent region server crash on every compaction
                // of this region.
                if (firstWriteToken)
                    setBypassPurgeWithWarning("More than one FIRST_WRITE_TOKEN.  commitTimestamp = " + commitTimestamp + ", beginTimestamp = " + beginTimestamp);
                assert !firstWriteToken;
                firstWriteToken = true;
                break;
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                // Assertions are only thrown on standalone.
                // On clusters, we want to bypass the purge to avoid
                // possible corruption, but also not error out, to
                // prevent region server crash on every compaction
                // of this region.
                if (deleteRightAfterFirstWriteTimestamp != 0)
                    setBypassPurgeWithWarning("More that one DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN.  commitTimestamp = " + commitTimestamp + ", beginTimestamp = " + beginTimestamp);
                assert deleteRightAfterFirstWriteTimestamp == 0;
                deleteRightAfterFirstWriteTimestamp = beginTimestamp;
                break;
            case USER_DATA: {
                if (purgeConfig.shouldPurgeUpdates() && commitTimestamp < purgeConfig.getTransactionLowWatermark()) {
                    EntryDecoder decoder = new EntryDecoder(element.getValueArray(), element.getValueOffset(), element.getValueLength());
                    BitIndex index = decoder.getCurrentIndex();
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG, "BitIndex: %s, length=%d", index, index.length());
                    boolean purge = !firstUpdateCell;
                    firstUpdateCell = false;
                    for (int col = index.nextSetBit(0); col >= 0; col = index.nextSetBit(col + 1)) {
                        if (!columnUpdateLatestTimestamp.containsKey(col)) {
                            columnUpdateLatestTimestamp.put(col, beginTimestamp);
                            purge = false;
                            if (LOG.isTraceEnabled())
                                SpliceLogUtils.trace(LOG, "Update cannot be purged: %s", element);
                        } else {
                            Long latestTimestamp = columnUpdateLatestTimestamp.get(col);
                            boolean beginTimestampIsLessThanLatestTimestamp =
                                    beginTimestamp < latestTimestamp;
                            if (!beginTimestampIsLessThanLatestTimestamp)
                                setBypassPurgeWithWarning("beginTimestamp not less than latestTimestamp.  beginTimestamp = " + beginTimestamp + ", latestTimestamp = " + latestTimestamp);
                            assert beginTimestampIsLessThanLatestTimestamp;
                        }
                    }
                    if (purge) {
                        boolean ret = updatesToPurgeTimestamps.add(beginTimestamp);
                        if (!ret)
                            setBypassPurgeWithWarning("Unable to add beginTimestamp to the updatesToPurgeTimestamps set.  beginTimestamp = " + beginTimestamp + ", updatesToPurgeTimestamps = " + updatesToPurgeTimestamps.toString());
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
        if (maxTombstone == null)
            setBypassPurgeWithWarning("maxTombstone is not set.");
        assert maxTombstone != null;
        switch (purgeConfig.getPurgeLatestTombstone()) {
            case ALWAYS:
                return true;
            case IF_DELETE_FOLLOWS_FIRST_WRITE:
                return firstWriteToken && !bypassPurge && deleteRightAfterFirstWriteTimestamp == maxTombstone.getTimestamp();
            case IF_FIRST_WRITE_PRESENT:
                return firstWriteToken && !bypassPurge;
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
