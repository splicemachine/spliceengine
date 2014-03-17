package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Helper class for FilterState. Captures the state associated with the current row being processed by the filter.
 */
public class FilterRowState<Result, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, Lock, OperationStatus> {

    /**
     * The key of the row currently being processed.
     */
    private DecodedCell currentRowKey = null;

    /**
     * Used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions .
     */
    Cell lastValidQualifier = null;

    /**
     * If a tombstone was detected on the row, then the associated timestamp will be stored here.
     */
    LongArrayList tombstoneTimestamps = new LongArrayList();
    LongArrayList antiTombstoneTimestamps = new LongArrayList();

    /**
     * The transactions that have been loaded as part of processing this row.
     */
    LongObjectOpenHashMap<Transaction> transactionCache = new LongObjectOpenHashMap<Transaction>();

    ObjectArrayList<Cell> commitTimestamps = ObjectArrayList.newInstance();

    boolean siTombstoneIncluded = false;

    boolean inData = false;
    Filter.ReturnCode shortCircuit = null;

    public FilterRowState() {
    }

    /**
     * Called for every key-value encountered by the filter. It is expected that key-values are read in row order.
     * Detects when the filter has moved to a new row and updates the state appropriately.
     */
    public void updateCurrentRow(DecodedCell keyValue) {
        if (currentRowKey == null) {
            currentRowKey = keyValue;
            lastValidQualifier = null;
            tombstoneTimestamps.clear();
            antiTombstoneTimestamps.clear();
            transactionCache.clear();
            commitTimestamps.clear();
            inData = false;
            shortCircuit = null;
        }
    }

    public void resetCurrentRow() {
        currentRowKey = null;
    }

    /**
     * Record that a tombstone marker was encountered on the current row.
     */
    public void setTombstoneTimestamp(long tombstoneTimestamp) {
        if (!antiTombstoneTimestamps.contains(tombstoneTimestamp)) {
            this.tombstoneTimestamps.add(tombstoneTimestamp);
        }
    }

    public void setAntiTombstoneTimestamp(long antiTombstoneTimestamp) {
        if (!tombstoneTimestamps.contains(antiTombstoneTimestamp)) {
            this.antiTombstoneTimestamps.add(antiTombstoneTimestamp);
        }
    }

    public void rememberCommitTimestamp(Cell keyValue) {
        commitTimestamps.add(keyValue);
    }

    public ObjectArrayList<Cell> getCommitTimestamps() {
        return commitTimestamps;
    }

    public boolean isSiTombstoneIncluded() {
        return siTombstoneIncluded;
    }

    public void setSiTombstoneIncluded() {
        this.siTombstoneIncluded = true;
    }
}
