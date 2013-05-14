package com.splicemachine.si.api;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRead;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.hbase.HScan;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor<PutOp, GetOp, ScanOp, MutationOp> extends ClientTransactor<PutOp, GetOp, ScanOp, MutationOp> {
    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    /**
     *
     * @param parent transaction that contains this new transaction
     * @param dependent indicator of whether this transaction can only finally commit if the parent does
     * @param allowWrites indicates whether this transaction can perform writes
     * @param readUncommitted
     * @param readCommitted
     * @return
     * @throws IOException
     */
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                        Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void keepAlive(TransactionId transactionId) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

    boolean processPut(STable table, RollForwardQueue rollForwardQueue, PutOp put) throws IOException;
    boolean isFilterNeededGet(GetOp get);
    boolean isFilterNeededScan(ScanOp scan);
    boolean isScanSIOnly(ScanOp scan);

    void preProcessGet(GetOp get) throws IOException;
    void preProcessScan(ScanOp scan) throws IOException;

    FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean siOnly) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException;

    void rollForward(STable table, long transactionId, List rows) throws IOException;
    SICompactionState newCompactionState();
}
