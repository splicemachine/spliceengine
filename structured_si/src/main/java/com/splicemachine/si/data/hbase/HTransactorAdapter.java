package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.*;
import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactor;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.SITransactor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public class HTransactorAdapter implements HTransactor,TransactorStatus {
    Transactor delegate;

    public HTransactorAdapter(Transactor delegate) {
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
    public TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException {
        return delegate.beginChildTransaction(parent, allowWrites);
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
    public boolean processPut(HRegion region, RollForwardQueue rollForwardQueue, Put put) throws IOException {
        return delegate.processPut(new HbRegion(region), rollForwardQueue, put);
    }

    @Override
    public boolean isFilterNeededGet(Get get) {
        return delegate.isFilterNeededGet(new HGet(get));
    }

    @Override
    public boolean isFilterNeededScan(Scan scan) {
        return delegate.isFilterNeededScan(new HScan(scan));
    }

    @Override
    public boolean isGetIncludeSIColumn(Get get) {
        return delegate.isGetIncludeSIColumn(new HGet(get));
    }

    @Override
    public boolean isScanIncludeSIColumn(Scan read) {
        return delegate.isScanIncludeSIColumn(new HScan(read));
    }

    @Override
    public boolean isScanIncludeUncommittedAsOfStart(Scan scan) {
        return delegate.isScanIncludeUncommittedAsOfStart(new HScan(scan));
    }

    @Override
    public void preProcessGet(Get get) throws IOException {
        delegate.preProcessGet(new HGet(get));
    }

    @Override
    public void preProcessScan(Scan scan) throws IOException {
        delegate.preProcessScan(new HScan(scan));
    }

    @Override
    public FilterState newFilterState(TransactionId transactionId) throws IOException {
        return delegate.newFilterState(transactionId);
    }

    @Override
    public FilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId,
                                      boolean includeSIColumn, boolean includeUncommittedAsOfStart) throws IOException {
        return delegate.newFilterState(rollForwardQueue, transactionId, includeSIColumn, includeUncommittedAsOfStart);
    }

    @Override
    public Filter.ReturnCode filterKeyValue(FilterState filterState, Object keyValue) throws IOException {
        return delegate.filterKeyValue(filterState, keyValue);
    }

    @Override
    public Result filterResult(FilterState filterState, Result result) throws IOException {
        return (Result) delegate.filterResult(filterState, result);
    }

    @Override
    public void rollForward(HRegion region, long transactionId, List rows) throws IOException {
        delegate.rollForward(new HbRegion(region), transactionId, rows);
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
    public TransactionId transactionIdFromGet(Get get) {
        return delegate.transactionIdFromGet(new HGet(get));
    }

    @Override
    public TransactionId transactionIdFromScan(Scan scan) {
        return delegate.transactionIdFromScan(new HScan(scan));
    }

    @Override
    public TransactionId transactionIdFromPut(Put put) {
        return delegate.transactionIdFromPut(put);
    }

    @Override
    public void initializeGet(String transactionId, Get get) throws IOException {
        delegate.initializeGet(transactionId, new HGet(get));
    }

    @Override
    public void initializeGet(String transactionId, Get get, boolean includeSIColumn) throws IOException {
        delegate.initializeGet(transactionId, get, includeSIColumn);
    }

    @Override
    public void initializeScan(String transactionId, Scan scan) {
        delegate.initializeScan(transactionId, new HScan(scan));
    }

    @Override
    public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn, boolean includeUncommittedAsOfStart) {
        delegate.initializeScan(transactionId, new HScan(scan), includeSIColumn, includeUncommittedAsOfStart);
    }

    @Override
    public void initializePut(String transactionId, Put put) {
        delegate.initializePut(transactionId, put);
    }

    @Override
    public Put createDeletePut(TransactionId transactionId, Object rowKey) {
        return (Put) delegate.createDeletePut(transactionId, rowKey);
    }

    @Override
    public boolean isDeletePut(Mutation put) {
        return delegate.isDeletePut(put);
    }

    @Override
    public long getTotalChildTransactions() {
        return ((TransactorStatus)delegate).getTotalChildTransactions();
    }

    @Override
    public long getTotalTransactions() {
        return ((TransactorStatus)delegate).getTotalTransactions();
    }

    @Override
    public long getTotalCommittedTransactions() {
        return ((TransactorStatus)delegate).getTotalCommittedTransactions();
    }

    @Override
    public long getTotalRolledBackTransactions() {
        return ((TransactorStatus)delegate).getTotalRolledBackTransactions();
    }

    @Override
    public long getTotalFailedTransactions() {
        return ((TransactorStatus)delegate).getTotalFailedTransactions();
    }

    @Override
    public TransactionStoreStatus getTransactionStoreStatus() {
        return ((SITransactor)delegate).getTransactionStore();
    }
}
