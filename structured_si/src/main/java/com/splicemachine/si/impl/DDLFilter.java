package com.splicemachine.si.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnStore;

import java.util.HashMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

public class DDLFilter implements Comparable<DDLFilter> {
    private final Transaction myTransaction;
	private final Transaction myParentTransaction;
//    private final TransactionStore transactionStore;
		private final TxnStore transactionStore;
		private Cache<String,Boolean> visibilityMap;
//    private ConcurrentMap<String, Boolean> visibilityMap;

		public DDLFilter(
						Transaction myTransaction,
						Transaction myParentTransaction,
						TxnStore transactionStore) {
				super();
				this.myTransaction = myTransaction;
				this.transactionStore = transactionStore;
				this.myParentTransaction = myParentTransaction;
				visibilityMap = CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(10000).build();
		}

		public boolean isVisibleBy(final String transactionId) throws IOException {
				Boolean visible = visibilityMap.getIfPresent(transactionId);
				if (visible != null) {
						return visible;
				}
				//if I didn't succeed, don't do anything
				if(myTransaction.getEffectiveStatus()!= TransactionStatus.COMMITTED) return false;
				//if I have a parent, and he was rolled back, don't do anything
				if(myParentTransaction!=null && myParentTransaction.getEffectiveStatus()==TransactionStatus.ROLLED_BACK) return false;
				try {
						return visibilityMap.get(transactionId,new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										throw new UnsupportedOperationException("IMPLEMENT");
//										Txn transaction  = transactionStore.getTransaction(Long.parseLong(transactionId));
//										Transaction transaction = transactionStore.getTransaction(new TransactionId(transactionId));
										/*
										 * For the purposes of DDL, we intercept any writes which occur AFTER us, regardless of
										 * my status.
										 *
										 * The reason for this is because the READ of us will not see those writes, which means
										 * that we need to intercept them and deal with them as if we were committed. If we rollback,
										 * then it shouldn't matter to the other operation (except for performance), and if we commit,
										 * then we should see properly constructed data.
										 */
//										long otherTxnId = transaction.getLongTransactionId();
//										visibilityMap.put(transactionId, myTransaction.getLongTransactionId() <= otherTxnId);
//										return myTransaction.getLongTransactionId()<=otherTxnId;
								}
						});
				} catch (ExecutionException e) {
						throw new IOException(e.getCause());
				}
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
