package com.splicemachine.si2.si.api;

import com.splicemachine.si2.data.api.STable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor {
    TransactionId beginTransaction();
    void commit(TransactionId transactionId) throws IOException;
    void abort(TransactionId transactionId);
    void fail(TransactionId transactionId);

    boolean processPut(STable table, Object put);
    boolean isFilterNeeded(Object operation);
    Object filterResult(FilterState filterState, Object result) throws IOException;

    FilterState newFilterState(STable table, TransactionId transactionId) throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue);
}
