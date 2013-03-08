package com.splicemachine.si2.si.api;

import com.splicemachine.si2.data.api.STable;

import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor {
    TransactionId beginTransaction();
    void commit(TransactionId transactionId);
    void abort(TransactionId transactionId);
    void fail(TransactionId transactionId);

    void processPuts(TransactionId transactionId, STable table, List puts);
    Object filterResult(TransactionId transactionId, Object result);
}
