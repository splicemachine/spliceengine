package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Scott Fines
 * Date: 6/19/14
 */
public class LazyTxn implements Txn {
		private volatile Txn delegate;
		private volatile boolean lookedUp = false;

		private final TxnSupplier store;
		private final long txnId;

		private final boolean hasDependent;
		private final boolean dependent;

		private final boolean hasAdditive;
		private final boolean additive;

		private final IsolationLevel isolationLevel;

		private volatile boolean inFinalState = false; //set to true if/when the lookup reveals the transaction is in the final state

		public LazyTxn(long txnId, TxnSupplier store) {
				this.txnId = txnId;
				this.store = store;
				this.hasDependent = false;
				this.dependent = false;

				this.hasAdditive = false;
				this.additive = false;
				this.isolationLevel = null;
		}

		public LazyTxn(TxnSupplier store,  long txnId,
									 IsolationLevel isolationLevel) {
				this.store = store;
				this.txnId = txnId;
				this.hasDependent = false;
				this.dependent = false;
				this.hasAdditive = false;
				this.additive = false;
				this.isolationLevel = isolationLevel;
		}

		public LazyTxn(TxnSupplier store,  long txnId,
									 boolean hasDependent, boolean dependent,
									 IsolationLevel isolationLevel) {
				this.store = store;
				this.txnId = txnId;
				this.hasDependent = hasDependent;
				this.dependent = dependent;
				this.hasAdditive = false;
				this.additive = false;
				this.isolationLevel = isolationLevel;
		}

		public LazyTxn(long txnId,TxnSupplier store,
									 boolean hasDependent, boolean dependent,
									 boolean hasAdditive, boolean additive,
									 IsolationLevel isolationLevel) {
				this.store = store;
				this.txnId = txnId;
				this.hasDependent = hasDependent;
				this.dependent = dependent;
				this.hasAdditive = hasAdditive;
				this.additive = additive;
				this.isolationLevel = isolationLevel;
		}

		@Override
		public long getGlobalCommitTimestamp() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getGlobalCommitTimestamp();
		}

		@Override
		public ConflictType conflicts(Txn otherTxn) {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.conflicts(otherTxn);
		}

		@Override
		public boolean isDependent() {
				if(hasDependent) return dependent;
				try {
						lookup(false); //don't need to force, since dependent never changes
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.isDependent();
		}

		@Override
		public boolean isAdditive() {
				if(hasAdditive) return additive;
				try {
						lookup(false); //don't need to force a refresh, since this property is constant for this transaction
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.isAdditive();
		}

		@Override
		public Collection<byte[]> getDestinationTables() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getDestinationTables();
		}

		@Override
		public boolean descendsFrom(Txn potentialParent) {
				try {
						lookup(false);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.descendsFrom(potentialParent);
		}

		@Override
		public State getEffectiveState() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getEffectiveState();
		}

		@Override
		public IsolationLevel getIsolationLevel() {
				if(isolationLevel!=null) return isolationLevel;
				try {
						lookup(false); //isolation levl never changes
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getIsolationLevel();
		}

		@Override
		public long getTxnId() {
				return txnId;
		}

		@Override
		public long getBeginTimestamp() {
				try {
						lookup(false); //begin timestamp never changes
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getBeginTimestamp();
		}

		@Override
		public long getCommitTimestamp() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getCommitTimestamp();
		}

		@Override
		public long getEffectiveCommitTimestamp() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getEffectiveCommitTimestamp();
		}

		@Override
		public long getEffectiveBeginTimestamp() {
				try{
						lookup(false);
				}catch(IOException ioe){
						throw new RuntimeException(ioe);
				}
				return delegate.getEffectiveBeginTimestamp();
		}

    @Override
    public long getLastKeepAliveTimestamp() {
        return -1l; //don't lookup just for the timestamp;
    }

    @Override
		public Txn getParentTransaction() {
				try {
						lookup(false); //parent txn never changes
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getParentTransaction();
		}

		@Override
		public State getState() {
				try {
						lookup(!inFinalState);
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.getState();
		}

		@Override
		public void commit() throws IOException {
				lookup(!inFinalState);
				delegate.commit();
		}

		@Override
		public void rollback() throws IOException {
				lookup(!inFinalState);
				delegate.rollback();
		}

		@Override
		public boolean allowsWrites() {
				try {
						lookup(false); //never changes
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				return delegate.allowsWrites();
		}

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				lookup(true);
				return delegate.elevateToWritable(writeTable);
		}

		@Override
		public boolean canSee(Txn otherTxn) {
				return false;
		}

		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (!(o instanceof Txn)) return false;

				Txn other = (Txn) o;

				return txnId == other.getTxnId();

		}

		@Override
		public int hashCode() {
				return (int) (txnId ^ (txnId >>> 32));
		}

		protected synchronized void lookup(boolean force) throws IOException {
				if(!lookedUp || force){
						delegate = store.getTransaction(txnId);
						lookedUp = true;
						inFinalState = delegate.getCommitTimestamp()>=0;
				}
		}
}
