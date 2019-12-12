package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;


class SICompactionStateMutate {
    private SortedSet<Cell> dataToReturn;
    private final boolean purgeDeletedRows;
    private long maxTombstoneTimestamp;

    SICompactionStateMutate(boolean purgeDeletedRows) {
        this.purgeDeletedRows = purgeDeletedRows;
        this.dataToReturn = new TreeSet<>(KeyValue.COMPARATOR);
        this.maxTombstoneTimestamp = 0;
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
        assert isSorted(rawList): "CompactionStateMutate: rawList not sorted";
        assert rawList.size() == txns.size();

        Iterator<TxnView> it = txns.iterator();
        for (Cell aRawList : rawList) {
            TxnView txn = it.next();
            mutate(aRawList, txn);
        }
        if (purgeDeletedRows && maxTombstoneTimestamp > 0) {
            removeTombStone(maxTombstoneTimestamp);
        }
        results.addAll(dataToReturn);
        assert isSorted(results): "CompactionStateMutate: results not sorted";
    }

    /**
     * Apply SI mutation logic to an individual key-value.
     */
    private void mutate(Cell element, TxnView txn) throws IOException {
        final CellType cellType= CellUtils.getKeyValueType(element);
        if (cellType == CellType.COMMIT_TIMESTAMP) {
            assert txn == null;
            dataToReturn.add(element);
            return;
        }
        if (txn == null) {
            // we don't have transactional information, just return the data as is
            dataToReturn.add(element);
            return;
        }
        if (txn.getState() == Txn.State.ROLLEDBACK) {
            // rolled back data, remove it from the compacted data
            return;
        }
        if (isCommitted(txn)) {
            /*
             * This element has been committed all the way to the user level, so a
             * commit timestamp can be placed on it.
             */
            long globalCommitTimestamp = txn.getEffectiveCommitTimestamp();
            dataToReturn.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(globalCommitTimestamp)));
            if (cellType == CellType.TOMBSTONE && element.getTimestamp() > maxTombstoneTimestamp) {
                maxTombstoneTimestamp = element.getTimestamp();
            }
        }
        // Committed or active, return the original data too
        dataToReturn.add(element);
    }

    private void removeTombStone(long maxTombstone) {
        SortedSet<Cell> cp = (SortedSet<Cell>)((TreeSet<Cell>)dataToReturn).clone();
        for (Cell element : cp) {
            long timestamp = element.getTimestamp();
            if (timestamp <= maxTombstone) {
                dataToReturn.remove(element);
            }
        }
    }

    static boolean isCommitted(TxnView txn) {
        while (txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() != Txn.ROOT_TRANSACTION) {
            txn = txn.getParentTxnView();
        }
        return txn.getState() == Txn.State.COMMITTED && txn.getParentTxnView() == Txn.ROOT_TRANSACTION;
    }

    private static Cell newTransactionTimeStampKeyValue(Cell element, byte[] value) {
        return new KeyValue(element.getRowArray(),
                element.getRowOffset(),
                element.getRowLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,0,1,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,
                element.getTimestamp(),KeyValue.Type.Put,
                value,0,value==null?0:value.length);
    }
}
