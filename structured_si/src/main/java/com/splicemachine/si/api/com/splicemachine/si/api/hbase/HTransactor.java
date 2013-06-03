package com.splicemachine.si.api.com.splicemachine.si.api.hbase;

import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public interface HTransactor extends HClientTransactor {
    TransactionId beginTransaction() throws IOException;

    TransactionId beginTransaction(boolean allowWrites) throws IOException;

    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                        Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void keepAlive(TransactionId transactionId) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;

    boolean processPut(HRegion region, RollForwardQueue rollForwardQueue, Put put) throws IOException;
    boolean isFilterNeededGet(Get get);
    boolean isFilterNeededScan(Scan scan);
    boolean isScanIncludeSIColumn(Scan scan);

    void preProcessGet(Get get) throws IOException;
    void preProcessScan(Scan scan) throws IOException;

    FilterState newFilterState(TransactionId transactionId) throws IOException;
    FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean includeSIColumn)
            throws IOException;
    Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException;
    Result filterResult(FilterState filterState, Result result) throws IOException;

    void rollForward(HRegion region, long transactionId, List rows) throws IOException;
    SICompactionState newCompactionState();
}
