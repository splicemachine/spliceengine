package com.splicemachine.si2.api;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor extends ClientTransactor {
    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                        Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void abort(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

    boolean processPut(STable table, Object put) throws IOException;
    boolean isFilterNeeded(Object operation);

    TransactionId getTransactionIdFromGet(Object get);
    void preProcessGet(SGet get) throws IOException;
    TransactionId getTransactionIdFromScan(Object scan);
    void preProcessScan(SScan scan) throws IOException;
    FilterState newFilterState(STable table, TransactionId transactionId) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException;
    Object filterResult(FilterState filterState, Object result) throws IOException;
}
