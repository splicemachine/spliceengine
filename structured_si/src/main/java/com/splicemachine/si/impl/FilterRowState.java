package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

public class FilterRowState {
    private Object currentRowKey = null;
    Object lastValidQualifier = null; // used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions
    Long tombstoneTimestamp = null;

    private final SDataLib dataLib;

    public FilterRowState(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    public void updateCurrentRow(Object rowKey) {
        if (currentRowKey == null || !dataLib.valuesEqual(currentRowKey, rowKey)) {
            currentRowKey = rowKey;
            lastValidQualifier = null;
            tombstoneTimestamp = null;
        }
    }

    public void setTombstoneTimestamp(long tombstoneTimestamp) {
        this.tombstoneTimestamp = tombstoneTimestamp;
    }
}
