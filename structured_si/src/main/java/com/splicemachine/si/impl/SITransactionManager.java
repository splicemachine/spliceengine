package com.splicemachine.si.impl;

import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactorListener;
import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.splicemachine.si.api.TransactionStatus.COMMITTED;
import static com.splicemachine.si.api.TransactionStatus.COMMITTING;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionManager implements TransactionManager {

		private final TransactionStore transactionStore;
		private final TimestampSource timestampSource;
		public static final int MAX_ACTIVE_COUNT = 1000; //TODO -sf- why this?

		private final TransactorListener listener;

		public SITransactionManager(TransactionStore transactionStore,
																TimestampSource timestampSource,
																TransactorListener listener) {
				this.transactionStore = transactionStore;
				this.timestampSource = timestampSource;
				this.listener = listener;
		}

		@Override
		public TransactionId beginTransaction() throws IOException {
				return beginTransaction(true);
		}

		@Override
		public TransactionId beginTransaction(boolean allowWrites) throws IOException {
				return beginTransaction(allowWrites, false, false);
		}

		@Override
		public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted)
						throws IOException {
				return beginChildTransaction(Transaction.rootTransaction.getTransactionId(), true, allowWrites, false, readUncommitted,
								readCommitted, null);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException {
				return beginChildTransaction(parent, true, allowWrites);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
				return beginChildTransaction(parent, dependent, allowWrites, false, null, null, null);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
																							 boolean additive, Boolean readUncommitted, Boolean readCommitted,
																							 TransactionId transactionToCommit) throws IOException {
				Long timestamp = commitIfNeeded(transactionToCommit);
				if (allowWrites || readCommitted != null || readUncommitted != null || timestamp != null) {
						final TransactionParams params = new TransactionParams(parent, dependent, allowWrites, additive, readUncommitted, readCommitted);
						if (timestamp == null) {
								timestamp = assignTransactionId();
						}
						final long beginTimestamp = generateBeginTimestamp(timestamp, params.parent.getId());
						transactionStore.recordNewTransaction(timestamp, params, TransactionStatus.ACTIVE, beginTimestamp, 0L);
						listener.beginTransaction(!parent.isRootTransaction());
						return new TransactionId(timestamp);
				} else {
						return createLightweightChildTransaction(parent.getId());
				}
		}

		@Override
		public void keepAlive(TransactionId transactionId) throws IOException {
				if (!transactionId.independentReadOnly) {
						transactionStore.recordTransactionKeepAlive(transactionId.getId());
				}
		}

		@Override
		public void commit(TransactionId transactionId) throws IOException {
				if (!transactionId.independentReadOnly) {
						final Transaction transaction = transactionStore.getTransaction(transactionId.getId());
						ensureTransactionActive(transaction);
						performCommit(transaction);
						listener.commitTransaction();
				}
		}

		@Override
		public void rollback(TransactionId transactionId) throws IOException {
				if (!transactionId.independentReadOnly) {
						listener.rollbackTransaction();
						rollbackDirect(transactionId.getId());
				}
		}

		@Override
		public void fail(TransactionId transactionId) throws IOException {
				if (!transactionId.independentReadOnly) {
						listener.failTransaction();
						transactionStore.recordTransactionStatusChange(transactionId.getId(),
										TransactionStatus.ACTIVE, TransactionStatus.ERROR);
				}
		}

		@Override
		public TransactionStatus getTransactionStatus(TransactionId transactionId) throws IOException {
				return transactionStore.getTransaction(transactionId).status;
		}

		@Override
		public TransactionId transactionIdFromString(String transactionId) {
				return new TransactionId(transactionId);
		}

		@Override
		public List<TransactionId> getActiveTransactionIds(TransactionId max) throws IOException {
				final long currentMin = timestampSource.retrieveTimestamp();
				final TransactionParams missingParams = new TransactionParams(Transaction.rootTransaction.getTransactionId(),
								true, false, false, false, false);
				@SuppressWarnings("unchecked") final List<Transaction> oldestActiveTransactions = transactionStore.getOldestActiveTransactions(
								currentMin, max.getId(), MAX_ACTIVE_COUNT, missingParams, TransactionStatus.ERROR);
				final List<TransactionId> result = new ArrayList<TransactionId>(oldestActiveTransactions.size());
				if (!oldestActiveTransactions.isEmpty()) {
						final long oldestId = oldestActiveTransactions.get(0).getTransactionId().getId();
						if (oldestId > currentMin) {
								timestampSource.rememberTimestamp(oldestId);
						}
						final TransactionId youngestId = oldestActiveTransactions.get(oldestActiveTransactions.size() - 1).getTransactionId();
						if (youngestId.equals(max)) {
								for (Transaction t : oldestActiveTransactions) {
										result.add(t.getTransactionId());
								}
						} else {
								throw new RuntimeException("expected max id of " + max + " but was " + youngestId);
						}
				}
				return result;
		}

		@Override
		public boolean forbidWrites(String tableName, TransactionId transactionId) throws IOException {
				return transactionStore.forbidPermission(tableName, transactionId);
		}

		/****************************************************************************************************/
		/*private helper methods*/

		/**
		 * Create a non-resource intensive child. This avoids hitting the transaction table. The same transaction ID is
		 * given to many callers, and calls to commit, rollback, etc are ignored.
		 */
		private TransactionId createLightweightChildTransaction(long parent) {
				return new TransactionId(parent, true);
		}

		private long assignTransactionId() throws IOException {
				return timestampSource.nextTimestamp();
		}

		private Long commitIfNeeded(TransactionId transactionToCommit) throws IOException {
				if (transactionToCommit == null) {
						return null;
				} else {
						return commitAndReturnTimestamp(transactionToCommit);
				}
		}

		private Long commitAndReturnTimestamp(TransactionId transactionToCommit) throws IOException {
				final ImmutableTransaction immutableTransaction = transactionStore.getImmutableTransaction(transactionToCommit);
				if (!immutableTransaction.getImmutableParent().isRootTransaction()) {
						throw new RuntimeException("Cannot begin a child transaction at the time a non-root transaction commits: "
										+ transactionToCommit.getTransactionIdString());
				}
				commit(transactionToCommit);
				final Transaction transaction = transactionStore.getTransaction(transactionToCommit.getId());
				final Long commitTimestampDirect = transaction.getCommitTimestampDirect();
				if (commitTimestampDirect.equals(transaction.getEffectiveCommitTimestamp())) {
						return commitTimestampDirect;
				} else {
						throw new RuntimeException("commit times did not match");
				}
		}

		/**
		 * Generate the next sequential timestamp / transaction ID.
		 *
		 * @return the new transaction ID.
		 */
		private long generateBeginTimestamp(long transactionId, long parentId) throws IOException {
				if (parentId == Transaction.ROOT_ID) {
						return transactionId;
				} else {
						return transactionStore.generateTimestamp(parentId);
				}
		}

		/**
		 * Update the transaction table to show this transaction is committed.
		 */
		private void performCommit(Transaction transaction) throws IOException {
				final long transactionId = transaction.getLongTransactionId();
				if (!transactionStore.recordTransactionStatusChange(transactionId, TransactionStatus.ACTIVE, COMMITTING)) {
						throw new IOException("committing failed");
				}
				Tracer.traceCommitting(transaction.getLongTransactionId());
				final Long globalCommitTimestamp = getGlobalCommitTimestamp(transaction);
				final long commitTimestamp = generateCommitTimestamp(transaction.getParent().getLongTransactionId());
				if (!transactionStore.recordTransactionCommit(transactionId, commitTimestamp, globalCommitTimestamp, COMMITTING, COMMITTED)) {
						throw new DoNotRetryIOException("commit failed");
				}
		}

		private Long getGlobalCommitTimestamp(Transaction transaction) throws IOException {
				if (transaction.needsGlobalCommitTimestamp()) {
						return timestampSource.nextTimestamp();
				} else {
						return null;
				}
		}

		private long generateCommitTimestamp(long parentId) throws IOException {
				if (parentId == Transaction.ROOT_ID) {
						return timestampSource.nextTimestamp();
				} else {
						return transactionStore.generateTimestamp(parentId);
				}
		}

		/**
		 * Throw an exception if the transaction is not active.
		 */
		private void ensureTransactionActive(Transaction transaction) throws IOException {
        /*
         * If the transaction is not finished, then it's active, so no worries.
         *
         * If it's committed, or COMMITTING, then it's also considered still active, so we
         * ignore it
         */
				if(transaction.status.isFinished()
								&& transaction.status!=TransactionStatus.COMMITTED
								&& transaction.status!=TransactionStatus.COMMITTING){
						String txnIdStr = transaction.getTransactionId().getTransactionIdString();
						throw new DoNotRetryIOException("transaction " + txnIdStr + " is not ACTIVE. State is " + transaction.status);
				}
		}

		private void rollbackDirect(long transactionId) throws IOException {
				Transaction transaction = transactionStore.getTransaction(transactionId);
				// currently the application above us tries to rollback already committed transactions.
				// This is poor form, but if it happens just silently ignore it.
				if (transaction.status.isActive()) {
						if (!transactionStore.recordTransactionStatusChange(transactionId,
										TransactionStatus.ACTIVE, TransactionStatus.ROLLED_BACK)) {
								throw new IOException("rollback failed");
						}
				}
		}
}
