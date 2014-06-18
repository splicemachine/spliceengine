package com.splicemachine.si.impl;

import com.splicemachine.si.api.AbstractTxn;
import com.splicemachine.si.api.Txn;

import java.io.IOException;

/**
 * Transaction which is partially constructed, but which looks up values in order
 * to complete itself when necessary.
 *
 * Useful primarily for child transactions during initial construction, in order
 * to populate default values.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class InheritingTxnView extends AbstractTxn {
		private final boolean hasDependent;
		private final boolean isDependent;
		private final boolean hasAdditive;
		private final boolean isAdditive;

		private final Txn parentTxn;
		private final long commitTimestamp;
		private final State state;
		private final boolean allowWrites;
		private final boolean hasAllowWrites;
		private final long globalCommitTimestamp;

		public InheritingTxnView(Txn parentTxn,
														 long txnId,long beginTimestamp,
														 IsolationLevel isolationLevel,
														 State state){
				this(parentTxn, txnId, beginTimestamp, isolationLevel,false,false,false,false,false,false,-1l,-1l,state);
		}

		public InheritingTxnView(Txn parentTxn,
														 long txnId, long beginTimestamp,
														 IsolationLevel isolationLevel,
														 boolean hasDependent, boolean isDependent,
														 boolean hasAdditive, boolean isAdditive,
														 boolean hasAllowWrites,boolean allowWrites,
														 long commitTimestamp,long globalCommitTimestamp,
														 State state) {
				super(txnId, beginTimestamp, isolationLevel);
				this.hasDependent = hasDependent;
				this.isDependent = isDependent;
				this.hasAdditive = hasAdditive;
				this.isAdditive = isAdditive;
				this.parentTxn = parentTxn;
				this.commitTimestamp = commitTimestamp;
				this.state = state;
				this.allowWrites = allowWrites;
				this.hasAllowWrites = hasAllowWrites;
				this.globalCommitTimestamp = globalCommitTimestamp;
		}

		@Override
		public boolean isDependent() {
				if(hasDependent) return isDependent;
				return parentTxn.isDependent();
		}

		@Override
		public boolean isAdditive() {
				if(hasAdditive) return isAdditive;
				return parentTxn.isAdditive();
		}

		@Override
		public long getGlobalCommitTimestamp() {
				if(globalCommitTimestamp==-1l) return parentTxn.getGlobalCommitTimestamp();
				return globalCommitTimestamp;
		}

		@Override
		public IsolationLevel getIsolationLevel() {
				if(isolationLevel!=null) return isolationLevel;
				return parentTxn.getIsolationLevel();
		}

		@Override public long getCommitTimestamp() { return commitTimestamp; }
		@Override public Txn getParentTransaction() { return parentTxn; }
		@Override public State getState() { return state; }

		@Override
		public long getEffectiveCommitTimestamp() {
				if(isDependent())
						return parentTxn.getEffectiveCommitTimestamp();
				return commitTimestamp;
		}

		@Override
		public boolean allowsWrites() {
				if(hasAllowWrites) return allowWrites;
				return parentTxn.allowsWrites();
		}

		@Override
		public void commit() throws IOException {
			throw new UnsupportedOperationException("Cannot commit a transaction view");
		}

		@Override
		public void rollback() throws IOException {
			throw new UnsupportedOperationException("Cannot rollback a transaction view");
		}

		@Override
		public void timeout() throws IOException {
				throw new UnsupportedOperationException("Cannot rollback a transaction view");
		}

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				throw new UnsupportedOperationException("Cannot elevate a transaction view");
		}
}
