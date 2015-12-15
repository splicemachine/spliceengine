package com.splicemachine.si.jmx;

import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TransactorListener;
import java.util.concurrent.atomic.AtomicLong;

public class ManagedTransactor<Mutation,OperationStatus,Put,RowLock,Table> implements TransactorListener, TransactorStatus {
    private Transactor<Put,RowLock> transactor;

    private final AtomicLong createdChildTxns = new AtomicLong(0l);

    private final AtomicLong createdTxns = new AtomicLong(0l);
    private final AtomicLong committedTxns = new AtomicLong(0l);
    private final AtomicLong rolledBackTxns = new AtomicLong(0l);
    private final AtomicLong failedTxns = new AtomicLong(0l);

    private final AtomicLong writes = new AtomicLong(0l);
    private final AtomicLong loadedTxns = new AtomicLong(0l);

    public Transactor<Put,RowLock> getTransactor() {
        return transactor;
    }

    public void setTransactor(Transactor transactor) {
        this.transactor = transactor;
    }

    // Implement TransactorListener

    @Override
    public void beginTransaction(boolean nested) {
        if(nested) {
            createdChildTxns.incrementAndGet();
        } else {
            createdTxns.incrementAndGet();
        }
    }

    @Override
    public void commitTransaction() {
        committedTxns.incrementAndGet();
    }

    @Override
    public void rollbackTransaction() {
        rolledBackTxns.incrementAndGet();
    }

    @Override
    public void failTransaction() {
        failedTxns.incrementAndGet();
    }

    @Override
    public void writeTransaction() {
        writes.incrementAndGet();
    }

    @Override
    public void loadTransaction() {
        loadedTxns.incrementAndGet();
    }

    // Implement TransactorStatus

    @Override
    public long getTotalChildTransactions() {
        return createdChildTxns.get();
    }

    @Override
    public long getTotalTransactions() {
        return createdTxns.get();
    }

    @Override
    public long getTotalCommittedTransactions() {
        return committedTxns.get();
    }

    @Override
    public long getTotalRolledBackTransactions() {
        return rolledBackTxns.get();
    }

    @Override
    public long getTotalFailedTransactions() {
        return failedTxns.get();
    }

    // Implement TransactionStoreStatus

    @Override
    public long getNumLoadedTxns() {
        return loadedTxns.get();
    }

    @Override
    public long getNumTxnUpdatesWritten() {
        return writes.get();
    }

}
