package com.splicemachine.si.api;

import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor<PutOp, GetOp, ScanOp, MutationOp, ResultType, KeyValue> extends ClientTransactor<PutOp, GetOp, ScanOp, MutationOp> {
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

    boolean processPut(STable table, RollForwardQueue rollForwardQueue, PutOp put) throws IOException;
    boolean isFilterNeededGet(GetOp get);
    boolean isFilterNeededScan(ScanOp scan);
    boolean isGetIncludeSIColumn(GetOp get);
    boolean isScanIncludeSIColumn(ScanOp scan);
    boolean isScanIncludeUncommittedAsOfStart(ScanOp scan);

    void preProcessGet(GetOp get) throws IOException;
    void preProcessScan(ScanOp scan) throws IOException;

    FilterState newFilterState(TransactionId transactionId) throws IOException;
    FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean includeSIColumn,
                               boolean includeUncommittedAsOfStart) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, KeyValue keyValue) throws IOException;
    ResultType filterResult(FilterState filterState, ResultType result) throws IOException;

    void rollForward(STable table, long transactionId, List rows) throws IOException;
    SICompactionState newCompactionState();
}
