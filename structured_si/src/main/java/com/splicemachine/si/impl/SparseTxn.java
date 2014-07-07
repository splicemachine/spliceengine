package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a Txn where some (optional) fields are populated.
 *
 * @author Scott Fines
 * Date: 6/30/14
 */
public class SparseTxn {

		private final long txnId;
		private final long beginTimestamp;
		private final long parentTxnId;
		private final long commitTimestamp;
		private final long globalCommitTimestamp;

		private final boolean hasDependentField;
		private final boolean dependent;
		private final boolean hasAdditiveField;
		private final boolean additive;

		private final Txn.IsolationLevel isolationLevel;
		private final Txn.State state;

		private final ByteSlice destTableBuffer;


		public SparseTxn(long txnId,
										 long beginTimestamp,
										 long parentTxnId,
										 long commitTimestamp,
										 long globalCommitTimestamp,
										 boolean hasDependentField, boolean dependent,
										 boolean hasAdditiveField, boolean additive,
										 Txn.IsolationLevel isolationLevel,
										 Txn.State state,
										 ByteSlice destTableBuffer) {
				this.txnId = txnId;
				this.beginTimestamp = beginTimestamp;
				this.parentTxnId = parentTxnId;
				this.commitTimestamp = commitTimestamp;
				this.globalCommitTimestamp = globalCommitTimestamp;
				this.hasDependentField = hasDependentField;
				this.dependent = dependent;
				this.hasAdditiveField = hasAdditiveField;
				this.additive = additive;
				this.isolationLevel = isolationLevel;
				this.state = state;
				this.destTableBuffer = destTableBuffer;
		}

		public ByteSlice getDestinationTableBuffer(){ return destTableBuffer;}
		public long getTxnId() { return txnId; }
		public long getBeginTimestamp() { return beginTimestamp; }
		public long getParentTxnId() { return parentTxnId; }

		/**
		 * @return the commit timestamp for this transaction, or -1 if the transaction has
		 * not been committed.
		 */
		public long getCommitTimestamp() { return commitTimestamp; }
		/**
		 * @return the "global" commit timestamp for this transaction, or -1 if the transaction
		 * has not been "globally" committed. If this transaction is a dependent child transaction,
		 * the global commit timestamp is equivalent to the commit timestamp of the highest ancestor, otherwise
		 * it is the same as the commit timestamp itself.
		 */
		public long getGlobalCommitTimestamp() { return globalCommitTimestamp; }

		public boolean hasDependentField() { return hasDependentField; }
		public boolean isDependent() { return dependent; }

		public boolean hasAdditiveField() { return hasAdditiveField; }
		public boolean isAdditive() { return additive; }

		/**
		 * @return the isolation level for this transaction, or {@code null} if no isolation level is explicitly set.
		 */
		public Txn.IsolationLevel getIsolationLevel() { return isolationLevel; }

		/**
		 * @return the current state of this transaction.
		 */
		public Txn.State getState() { return state; }
}
