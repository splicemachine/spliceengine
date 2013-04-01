package com.splicemachine.si2.si.api;

import com.splicemachine.si2.data.api.STable;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor {
    TransactionId beginTransaction(boolean allowWrites) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void abort(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

    boolean processPut(STable table, Object put) throws IOException;
    boolean isFilterNeeded(Object operation);

    FilterState newFilterState(STable table, TransactionId transactionId) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException;
    Object filterResult(FilterState filterState, Object result) throws IOException;
}
