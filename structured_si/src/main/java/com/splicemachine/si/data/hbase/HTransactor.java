package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

public class HTransactor<PutOp extends Put, GetOp extends Get, ScanOp extends Scan, MutationOp extends Mutation>
        implements Transactor<PutOp, GetOp, ScanOp, MutationOp> {
    Transactor delegate;

    public HTransactor(Transactor delegate) {
        this.delegate = delegate;
    }

    @Override
    public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException {
        return delegate.beginTransaction(allowWrites, readUncommitted, readCommitted);
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
        return delegate.processPut(table, rollForwardQueue, put);
    }

    @Override
    public boolean isFilterNeededGet(GetOp get) {
        return delegate.isFilterNeededGet(new HGet(get));
    }

    @Override
    public boolean isFilterNeededScan(ScanOp scan) {
        return delegate.isFilterNeededScan(new HScan(scan));
    }

    @Override
    public boolean isScanSIOnly(ScanOp read) {
        return delegate.isScanSIOnly(new HScan(read));
    }

    @Override
    public void preProcessGet(GetOp get) throws IOException {
        delegate.preProcessGet(new HGet(get));
    }

    @Override
    public void preProcessScan(ScanOp scan) throws IOException {
        delegate.preProcessScan(new HScan(scan));
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
        return delegate.transactionIdFromGet(new HGet(get));
    }

    @Override
    public TransactionId transactionIdFromScan(ScanOp scan) {
        return delegate.transactionIdFromScan(new HScan(scan));
    }

    @Override
    public TransactionId transactionIdFromPut(PutOp put) {
        return delegate.transactionIdFromPut(put);
    }

    @Override
    public void initializeGet(String transactionId, Get get) throws IOException {
        delegate.initializeGet(transactionId, new HGet(get));
    }

    @Override
    public void initializeScan(String transactionId, ScanOp scan) {
        delegate.initializeScan(transactionId, new HScan(scan));
    }

    @Override
    public void initializeScan(String transactionId, ScanOp scan, boolean siOnly) {
        delegate.initializeScan(transactionId, new HScan(scan), siOnly);
    }

    @Override
    public void initializePut(String transactionId, PutOp put) {
        delegate.initializePut(transactionId, put);
    }

    @Override
    public PutOp createDeletePut(TransactionId transactionId, Object rowKey) {
        return (PutOp) delegate.createDeletePut(transactionId, rowKey);
    }

    @Override
    public boolean isDeletePut(MutationOp put) {
        return delegate.isDeletePut(put);
    }
}
