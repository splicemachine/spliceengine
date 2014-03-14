package com.splicemachine.si.impl;

import java.io.IOException;

public class DDLFilter implements Comparable<DDLFilter> {
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
				/*
				 * For the purposes of DDL, we intercept any writes which occur AFTER us, regardless of
				 * my status.
				 *
				 * The reason for this is because the READ of us will not see those writes, which means
				 * that we need to intercept them and deal with them as if we were committed. If we rollback,
				 * then it shouldn't matter to the other operation (except for performance), and if we commit,
				 * then we should see properly constructed data.
				 */
				long otherTxnId = transaction.getLongTransactionId();
				return myTransaction.getLongTransactionId()<=otherTxnId;
//				// TODO use cache here
//        return transaction.canSee(myTransaction, new TransactionSource() {
//            @Override
//            public Transaction getTransaction(long timestamp) throws IOException {
//                return transactionStore.getTransaction(timestamp);
//            }
//        }).visible;
    }

    public Transaction getTransaction() {
        return myTransaction;
    }

    @Override
    public int compareTo(DDLFilter o) {
        if (o == null) {
            return 1;
        }
        if (myTransaction.getStatus().isCommitted()) {
            if (o.getTransaction().getStatus().isCommitted()) {
                return Long.compare(myTransaction.getCommitTimestampDirect(), o.getTransaction().getCommitTimestampDirect());
            } else {
                return 1;
            }
        } else {
            if (o.getTransaction().getStatus().isCommitted()) {
                return -1;
            } else {
                return Long.compare(myTransaction.getEffectiveBeginTimestamp(), o.getTransaction().getEffectiveCommitTimestamp());
            }
        }
    }
}
