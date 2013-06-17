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
    /**
     * Start a writable transaction in "snapshot isolation" concurrency mode.
     */
    TransactionId beginTransaction() throws IOException;

    /**
     * Start a transaction in "snapshot isolation" concurrency mode.
     */
    TransactionId beginTransaction(boolean allowWrites) throws IOException;

    /**
     * Start a transaction with the specified isolation mode.
     * @param allowWrites boolean indicator of whether the new transaction is allowed to write
     * @param readUncommitted indicator of whether to read data from other, uncommitted transactions
     * @param readCommitted indicator of whether to read data from committed transactions that occur after this new
     *                      transaction is begun
     */
    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException;
    /**
     *
     *
     * @param parent transaction that contains this new transaction
     * @param dependent indicator of whether this transaction can only finally commit if the parent does
     * @param allowWrites indicates whether this transaction can perform writes
     * @return
     * @throws IOException
     */
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                               Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void keepAlive(TransactionId transactionId) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

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
