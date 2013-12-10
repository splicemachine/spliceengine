package com.splicemachine.si.impl;

import java.io.IOException;

public class DDLFilter {
    private final Transaction myTransaction;
    private final TransactionStore transactionStore;

    public DDLFilter(
            Transaction myTransaction,
            TransactionStore transactionStore) {
        super();
        this.myTransaction = myTransaction;
        this.transactionStore = transactionStore;
    }

    public boolean isVisibleBy(String transactionId) throws IOException {
        Transaction transaction = transactionStore.getTransaction(new TransactionId(transactionId));
        // TODO use cache here
        return transaction.canSee(myTransaction, new TransactionSource() {
            @Override
            public Transaction getTransaction(long timestamp) throws IOException {
                return transactionStore.getTransaction(timestamp);
            }
        }).visible;
    }

    public Transaction getTransaction() {
        return myTransaction;
    }
}
