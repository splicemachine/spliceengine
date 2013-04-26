package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

import java.util.HashMap;
import java.util.Map;

public class FilterRowState {
    private Object currentRowKey = null;
    Object lastValidQualifier = null; // used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions
    Long tombstoneTimestamp = null;
    Map<Long, Transaction> transactionCache;

    private final SDataLib dataLib;

    public FilterRowState(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    public void updateCurrentRow(Object rowKey) {
        if (currentRowKey == null || !dataLib.valuesEqual(currentRowKey, rowKey)) {
            currentRowKey = rowKey;
            lastValidQualifier = null;
            tombstoneTimestamp = null;
            transactionCache = new HashMap<Long, Transaction>();
        }
    }

    public void setTombstoneTimestamp(long tombstoneTimestamp) {
        this.tombstoneTimestamp = tombstoneTimestamp;
    }
}
