package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for FilterState. Captures the state associated with the current row being processed by the filter.
 */
public class FilterRowState<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, OperationStatus> {
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;

    /**
     * The key of the row currently being processed.
     */
    private Data currentRowKey = null;

    /**
     * Used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions .
     */
    KeyValue lastValidQualifier = null;

    /**
     * If a tombstone was detected on the row, then the associated timestamp will be stored here.
     */
    List<Long> tombstoneTimestamps = new ArrayList<Long>();
    List<Long> antiTombstoneTimestamps = new ArrayList<Long>();

    /**
     * The transactions that have been loaded as part of processing this row.
     */
    Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    List<KeyValue> commitTimestamps = new ArrayList<KeyValue>();

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
    public void updateCurrentRow(DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, OperationStatus> keyValue) {
        if (currentRowKey == null) {
            currentRowKey = keyValue.row();
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

    public List<KeyValue> getCommitTimestamps() {
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
