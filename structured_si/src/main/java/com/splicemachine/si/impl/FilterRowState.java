package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Helper class for FilterState. Captures the state associated with the current row being processed by the filter.
 */
public class FilterRowState<Result, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, Lock, OperationStatus> {
    private final SDataLib<Put, Delete, Get, Scan> dataLib;

    /**
     * The key of the row currently being processed.
     */
    private DecodedKeyValue<Result, Put, Delete, Get, Scan> currentRowKey = null;

    /**
     * Used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions .
     */
    KeyValue lastValidQualifier = null;

    /**
     * If a tombstone was detected on the row, then the associated timestamp will be stored here.
     */
    LongArrayList tombstoneTimestamps = new LongArrayList();
    LongArrayList antiTombstoneTimestamps = new LongArrayList();

    /**
     * The transactions that have been loaded as part of processing this row.
     */
    LongObjectOpenHashMap<Transaction> transactionCache = new LongObjectOpenHashMap<Transaction>();

    ObjectArrayList<KeyValue> commitTimestamps = ObjectArrayList.newInstance();

    boolean siColumnIncluded = false;
    boolean siTombstoneIncluded = false;

    boolean inData = false;
    Filter.ReturnCode shortCircuit = null;

    public FilterRowState(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    /**
     * Called for every key-value encountered by the filter. It is expected that key-values are read in row order.
     * Detects when the filter has moved to a new row and updates the state appropriately.
     */
    public void updateCurrentRow(DecodedKeyValue<Result, Put, Delete, Get, Scan> keyValue) {
        if (currentRowKey == null) {
            currentRowKey = keyValue;
            lastValidQualifier = null;
            tombstoneTimestamps.clear();
            antiTombstoneTimestamps.clear();
            transactionCache.clear();
            commitTimestamps.clear();
            siColumnIncluded = false;
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

    public void rememberCommitTimestamp(KeyValue keyValue) {
        commitTimestamps.add(keyValue);
    }

    public ObjectArrayList<KeyValue> getCommitTimestamps() {
        return commitTimestamps;
    }

    public boolean isSiColumnIncluded() {
        return siColumnIncluded;
    }

    public void setSiColumnIncluded() {
        this.siColumnIncluded = true;
    }

    public boolean isSiTombstoneIncluded() {
        return siTombstoneIncluded;
    }

    public void setSiTombstoneIncluded() {
        this.siTombstoneIncluded = true;
    }
}
