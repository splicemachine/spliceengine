package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.TransactionId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SiFilterState implements FilterState {
    final STable table;
    final TransactionId transactionId;
    final TransactionStruct transactionStruct;

    Object currentRowKey;
    Map<Long, TransactionStruct> committedTransactions;
    Map<Long, TransactionStruct> stillRunningTransactions;
    Map<Long, TransactionStruct> failedTransactions;
    Object lastValidQualifier; // used to emulate the INCLUDE_AND_NEXT_COLUMN ReturnCode that is in later HBase versions
    Long tombstoneTimestamp = null;

    public SiFilterState(STable table, TransactionStruct transactionStruct) {
        this.table = table;
        this.transactionId = new SiTransactionId(transactionStruct.beginTimestamp);
        this.transactionStruct = transactionStruct;
        committedTransactions = new HashMap<Long, TransactionStruct>();
        stillRunningTransactions = new HashMap<Long, TransactionStruct>();
        failedTransactions = new HashMap<Long, TransactionStruct>();
    }
}
