package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

import java.util.HashMap;
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
    Long tombstoneTimestamp = null;

    /**
     * The transactions that have been loaded as part of processing this row.
     */
    Map<Long, Transaction> transactionCache;

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
            tombstoneTimestamp = null;
            transactionCache = new HashMap<Long, Transaction>();
        }
    }

    /**
     * Record that a tombstone marker was encountered on the current row.
     */
    public void setTombstoneTimestamp(long tombstoneTimestamp) {
        this.tombstoneTimestamp = tombstoneTimestamp;
    }
}
