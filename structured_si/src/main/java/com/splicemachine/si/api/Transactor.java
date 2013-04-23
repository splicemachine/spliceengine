package com.splicemachine.si.api;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRead;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor extends ClientTransactor {
    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    /**
     * @param parent          transaction that contains this new transaction
     * @param dependent       indicator of whether this transaction can only finally commit if the parent does
     * @param allowWrites     indicates whether this transaction can peform writes
     * @param readUncommitted
     * @param readCommitted
     * @return
     * @throws IOException
     */
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                        Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

    boolean processPut(STable table, Object put, PutLog putLog) throws IOException;
    void rollForward(STable table, PutLog putLog, long transactionId) throws IOException;

    boolean isFilterNeeded(Object operation);

    void preProcessRead(SRead readOperation) throws IOException;

    FilterState newFilterState(PutLog putLog, TransactionId transactionId) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException;
}
