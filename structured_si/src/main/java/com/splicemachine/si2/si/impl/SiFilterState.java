package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TransactionId;

import java.util.HashMap;
import java.util.Map;

public class SiFilterState implements FilterState {
    final STable table;
    final TransactionId transactionId;

    Object currentRowKey;
    Map<Long,Long> committedTransactions;
    Object lastValidQualifier; // used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions
    Long tombstoneTimestamp = null;

    public SiFilterState(STable table, TransactionId transactionId) {
        this.table = table;
        this.transactionId = transactionId;
    }
}
