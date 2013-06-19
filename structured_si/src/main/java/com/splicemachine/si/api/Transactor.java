package com.splicemachine.si.api;

import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor<Table, Put, Get, Scan, Mutation, Result, KeyValue, Data, Hashable> extends ClientTransactor<Put, Get, Scan, Mutation, Data> {

    boolean processPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException;
    boolean isFilterNeededGet(Get get);
    boolean isFilterNeededScan(Scan scan);
    boolean isGetIncludeSIColumn(Get get);
    boolean isScanIncludeSIColumn(Scan scan);
    boolean isScanIncludeUncommittedAsOfStart(Scan scan);

    void preProcessGet(Get get) throws IOException;
    void preProcessScan(Scan scan) throws IOException;

    FilterState newFilterState(TransactionId transactionId) throws IOException;
    FilterState newFilterState(RollForwardQueue<Data, Hashable> rollForwardQueue, TransactionId transactionId, boolean includeSIColumn,
                               boolean includeUncommittedAsOfStart) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, KeyValue keyValue) throws IOException;
    Result filterResult(FilterState filterState, Result result) throws IOException;

    void rollForward(Table table, long transactionId, List<Data> rows) throws IOException;
    SICompactionState newCompactionState();
}
