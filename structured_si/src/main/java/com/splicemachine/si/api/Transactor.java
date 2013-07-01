package com.splicemachine.si.api;

import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor<Table, Put, Get, Scan, Mutation, Result, KeyValue, Data, Hashable, Lock>
        extends ClientTransactor<Put, Get, Scan, Mutation, Data> {

    /**
     * Execute the put operation (with SI treatment) on the table. Send roll-forward notifications to the rollForwardQueue.
     */
    boolean processPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException;
    PutToRun<Mutation> preProcessBatchPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put,
                                          Map<Hashable, Lock> locks) throws IOException;
    void postProcessBatchPut(Table table, Put put, Lock lock, Set<Long> conflictingChildren) throws IOException;

    /**
     * Look at the operation and report back whether it has been flagged for SI treatment.
     */
    boolean isFilterNeededGet(Get get);
    boolean isFilterNeededScan(Scan scan);

    /**
     * Determine whether an operation has been marked to indicate that it should return an SI column.
     */
    boolean isGetIncludeSIColumn(Get get);
    boolean isScanIncludeSIColumn(Scan scan);

    boolean isScanIncludeUncommittedAsOfStart(Scan scan);

    /**
     * Perform server-side pre-processing of operations. This is before they are actually executed.
     */
    void preProcessGet(Get get) throws IOException;
    void preProcessScan(Scan scan) throws IOException;

    /**
     * Construct an object to track the stateful aspects of a scan. Each key value from the scan will be considered
     * in light of this state.
     */
    FilterState newFilterState(TransactionId transactionId) throws IOException;
    FilterState newFilterState(RollForwardQueue<Data, Hashable> rollForwardQueue, TransactionId transactionId, boolean includeSIColumn,
                               boolean includeUncommittedAsOfStart) throws IOException;

    /**
     * Consider whether to use a key value in light of a given filterState.
     */
    Filter.ReturnCode filterKeyValue(FilterState filterState, KeyValue keyValue) throws IOException;

    /**
     * This is for use in code outside of a proper HBase filter that wants to apply the equivalent of SI filter logic.
     * Pass in an entire result object that contains all of the key values for a row. Receive back a new result that only
     * contains the key values that should be visible in light of the filterState.
     */
    Result filterResult(FilterState filterState, Result result) throws IOException;

    /**
     * Attempt to update all of the data rows on the table to reflect the final status of the transaction with the given
     * transactionId.
     */
    void rollForward(Table table, long transactionId, List<Data> rows) throws IOException;

    /**
     * Create an object to keep track of the state of an HBase table compaction operation.
     */
    SICompactionState newCompactionState();

    /**
     * Consider a list of key values in light of the compaction state and populate the result list with the key values
     * that should be used to represent this data in the newly compacted table.
     */
    void compact(SICompactionState compactionState, List<KeyValue> rawList, List<KeyValue> results) throws IOException;
}
