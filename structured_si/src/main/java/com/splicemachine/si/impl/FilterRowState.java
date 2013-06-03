package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for SIFilterState. Captures the state associated with the current row being processed by the filter.
 */
public class FilterRowState {
    private final SDataLib dataLib;

    /**
     * The key of the row currently being processed.
     */
    private Object currentRowKey = null;

    /**
     * Used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions .
     */
    Object lastValidQualifier = null;

    /**
     * If a tombstone was detected on the row, then the associated timestamp will be stored here.
     */
    List<Long> tombstoneTimestamps = null;

    /**
     * The transactions that have been loaded as part of processing this row.
     */
    Map<Long, Transaction> transactionCache;

    List<DecodedKeyValue> commitTimestamps = new ArrayList<DecodedKeyValue>();

    boolean siColumnIncluded = false;

    public FilterRowState(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    /**
     * Called for every key-value encountered by the filter. It is expected that key-values are read in row order.
     * Detects when the filter has moved to a new row and updates the state appropriately.
     */
    public void updateCurrentRow(Object rowKey) {
        if (currentRowKey == null || !dataLib.valuesEqual(currentRowKey, rowKey)) {
            currentRowKey = rowKey;
            lastValidQualifier = null;
            tombstoneTimestamps = new ArrayList<Long>();
            transactionCache = new HashMap<Long, Transaction>();
            commitTimestamps = new ArrayList<DecodedKeyValue>();
            siColumnIncluded = false;
        }
    }

    /**
     * Record that a tombstone marker was encountered on the current row.
     */
    public void setTombstoneTimestamp(long tombstoneTimestamp) {
        this.tombstoneTimestamps.add(tombstoneTimestamp);
    }

    public void rememberCommitTimestamp(DecodedKeyValue keyValue) {
        commitTimestamps.add(keyValue);
    }

    public List<DecodedKeyValue> getCommitTimestamps() {
        return commitTimestamps;
    }

    public boolean isSiColumnIncluded() {
        return siColumnIncluded;
    }

    public void setSiColumnIncluded() {
        this.siColumnIncluded = true;
    }

}
