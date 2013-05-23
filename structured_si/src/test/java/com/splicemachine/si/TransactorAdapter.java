package com.splicemachine.si;

import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.hbase.HGet;
import com.splicemachine.si.data.hbase.HScan;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

public class TransactorAdapter<PutOp, GetOp, ScanOp, MutationOp, ResultType>
        implements Transactor<PutOp, GetOp, ScanOp, MutationOp, ResultType> {
    private Transactor<Put, Get, Scan, Mutation, Result> delegate;

    public TransactorAdapter(Transactor<Put, Get, Scan, Mutation, Result> delegate) {
        this.delegate = delegate;
    }

    @Override
    public TransactionId beginTransaction() throws IOException {
        return delegate.beginTransaction();
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites) throws IOException {
        return delegate.beginTransaction(allowWrites);
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException {
        return delegate.beginTransaction(allowWrites, readUncommitted, readCommitted);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
        return delegate.beginChildTransaction(parent, dependent, allowWrites);
    }

    @Override
    public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites, Boolean readUncommitted, Boolean readCommitted) throws IOException {
        return delegate.beginChildTransaction(parent, dependent, allowWrites, readUncommitted, readCommitted);
    }

    @Override
    public void keepAlive(TransactionId transactionId) throws IOException {
        delegate.keepAlive(transactionId);
    }

    @Override
    public void commit(TransactionId transactionId) throws IOException {
        delegate.commit(transactionId);
    }

    @Override
    public void rollback(TransactionId transactionId) throws IOException {
        delegate.rollback(transactionId);
    }

    @Override
    public void fail(TransactionId transactionId) throws IOException {
        delegate.fail(transactionId);
    }

    @Override
    public boolean processPut(STable table, RollForwardQueue rollForwardQueue, PutOp put) throws IOException {
        return delegate.processPut(table, rollForwardQueue, (Put) put);
    }

    @Override
    public boolean isFilterNeededGet(GetOp get) {
        return delegate.isFilterNeededGet(prepGet(get));
    }

    @Override
    public boolean isFilterNeededScan(ScanOp scan) {
        return delegate.isFilterNeededScan(prepScan(scan));
    }

    @Override
    public boolean isScanSIFamilyOnly(ScanOp scan) {
        return delegate.isScanSIFamilyOnly(prepScan(scan));
    }

    @Override
    public void preProcessGet(GetOp get) throws IOException {
        delegate.preProcessGet(prepGet(get));
    }

    @Override
    public void preProcessScan(ScanOp scan) throws IOException {
        delegate.preProcessScan(prepScan(scan));
    }

    @Override
    public FilterState newFilterState(TransactionId transactionId) throws IOException {
        return delegate.newFilterState(transactionId);
    }

    @Override
    public FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean siOnly) throws IOException {
        return delegate.newFilterState(rollForwardQueue, transactionId, siOnly);
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        return delegate.filterKeyValue(filterState, keyValue);
    }

    @Override
    public ResultType filterResult(FilterState filterState, ResultType result) throws IOException {
        return (ResultType) delegate.filterResult(filterState, (Result) result);
    }

    @Override
    public void rollForward(STable table, long transactionId, List rows) throws IOException {
        delegate.rollForward(table, transactionId, rows);
    }

    @Override
    public SICompactionState newCompactionState() {
        return delegate.newCompactionState();
    }

    @Override
    public TransactionId transactionIdFromString(String transactionId) {
        return delegate.transactionIdFromString(transactionId);
    }

    @Override
    public TransactionId transactionIdFromGet(GetOp get) {
        return delegate.transactionIdFromGet(prepGet(get));
    }

    @Override
    public TransactionId transactionIdFromScan(ScanOp scan) {
        return delegate.transactionIdFromScan(prepScan(scan));
    }

    @Override
    public TransactionId transactionIdFromPut(PutOp put) {
        return delegate.transactionIdFromPut((Put) put);
    }

    @Override
    public void initializeGet(String transactionId, GetOp get) throws IOException {
        delegate.initializeGet(transactionId, prepGet(get));
    }

    @Override
    public void initializeScan(String transactionId, ScanOp scan) {
        delegate.initializeScan(transactionId, prepScan(scan));
    }

    @Override
    public void initializeScan(String transactionId, ScanOp scan, boolean siFamilyOnly) {
        delegate.initializeScan(transactionId, prepScan(scan), siFamilyOnly);
    }

    @Override
    public void initializePut(String transactionId, PutOp put) {
        delegate.initializePut(transactionId, (Put) put);
    }

    @Override
    public PutOp createDeletePut(TransactionId transactionId, Object rowKey) {
        return (PutOp) delegate.createDeletePut(transactionId, rowKey);
    }

    public boolean isDeletePut(MutationOp put) {
        return delegate.isDeletePut((Mutation) put);
    }

    private Get prepGet(Object get) {
        if (get instanceof HGet) {
            return ((HGet) get).getGet();
        } else {
            return (Get) get;
        }
    }

    private Scan prepScan(Object scan) {
        if (scan instanceof HScan) {
            return ((HScan) scan).getScan();
        } else {
            return (Scan) scan;
        }
    }


}
