package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionStatus;
import java.util.HashMap;
import java.io.IOException;

public class DDLFilter implements Comparable<DDLFilter> {
    private final Transaction myTransaction;
	private final Transaction myParentTransaction;
    private final TransactionStore transactionStore;
    private HashMap<String, Boolean> visibilityMap;

    public DDLFilter(
            Transaction myTransaction,
		    Transaction myParentTransaction,
            TransactionStore transactionStore) {
        super();
        this.myTransaction = myTransaction;
        this.transactionStore = transactionStore;
		this.myParentTransaction = myParentTransaction;
        visibilityMap = new HashMap<String, Boolean>();
    }

    public boolean isVisibleBy(String transactionId) throws IOException {
                Boolean visible = visibilityMap.get(transactionId);
                if (visible != null) {
                    return visible.booleanValue();
                }
				//if I didn't succeed, don't do anything
				if(myTransaction.getEffectiveStatus()!= TransactionStatus.COMMITTED) return false;
				//if I have a parent, and he was rolled back, don't do anything
				if(myParentTransaction!=null && myParentTransaction.getEffectiveStatus()==TransactionStatus.ROLLED_BACK) return false;

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
                visibilityMap.put(transactionId, new Boolean(myTransaction.getLongTransactionId()<=otherTxnId));
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
                return compare(myTransaction.getCommitTimestampDirect(), o.getTransaction().getCommitTimestampDirect());
            } else {
                return 1;
            }
        } else {
            if (o.getTransaction().getStatus().isCommitted()) {
                return -1;
            } else {
                return compare(myTransaction.getEffectiveBeginTimestamp(), o.getTransaction().getEffectiveBeginTimestamp());
            }
        }
    }

    private static int compare(long my, long other) {
        if (my > other) {
            return 1;
        } else if (my < other) {
            return -1;
        } else {
            return 0;
        }
    }
}
