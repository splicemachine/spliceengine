package com.splicemachine.si.impl;

import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 * Date: 6/18/14
 */
public class ReadOnlyTxn extends AbstractTxn {
		private final Txn parentTxn;
		private AtomicReference<State> state = new AtomicReference<State>(State.ACTIVE);

		private final TxnLifecycleManager tc;
		private final boolean dependent;
		private final boolean additive;

		public static ReadOnlyTxn createReadOnlyChildTransaction(
						Txn parentTxn,
						TxnLifecycleManager tc,
						boolean dependent,
						boolean additive){
				return new ReadOnlyTxn(-1l,
								parentTxn.getBeginTimestamp(),
								parentTxn.getIsolationLevel(),parentTxn,tc,dependent,additive);
		}
		public static ReadOnlyTxn createReadOnlyParentTransaction(long txnId,long beginTimestamp,
																															IsolationLevel isolationLevel,
																															TxnLifecycleManager tc,
																															boolean additive){
				return new ReadOnlyTxn(txnId,beginTimestamp,isolationLevel, ROOT_TRANSACTION,tc,false,additive);
		}

		protected ReadOnlyTxn(long txnId,
											 long beginTimestamp,
											 IsolationLevel isolationLevel,
											 Txn parentTxn,
											 TxnLifecycleManager tc,
											 boolean dependent,
											 boolean additive) {
				super(txnId, beginTimestamp, isolationLevel);
				this.parentTxn = parentTxn;
				this.tc = tc;
				this.additive = additive;

				this.dependent = dependent;
		}

		@Override public boolean isDependent() { return dependent; }

		@Override
		public boolean isAdditive() {
				return additive;
		}

		@Override
		public long getCommitTimestamp() {
				return -1l; //read-only transactions do not need to commit
		}

		@Override
		public long getGlobalCommitTimestamp() {
				return -1l; //read-only transactions do not need a global commit timestamp
		}

		@Override
		public long getEffectiveCommitTimestamp() {
				if(parentTxn!=null)
						return parentTxn.getEffectiveCommitTimestamp();
				return -1l; //read-only transactions do not need to commit, so they don't need a TxnId
		}

		@Override public Txn getParentTransaction() { return parentTxn; }

		@Override public State getState() { return state.get(); }

		@Override
		public void commit() throws IOException {
				boolean shouldContinue;
				do{
						State currState = state.get();
						switch(currState){
								case COMMITTED:
										return;
								case ROLLEDBACK:
										throw new CannotCommitException(txnId,currState);
								default:
										shouldContinue = state.compareAndSet(currState,State.COMMITTED);
						}
				}while(shouldContinue);
		}

		@Override
		public void rollback() throws IOException {
				boolean shouldContinue;
				do{
						State currState = state.get();
						switch(currState){
								case COMMITTED:
								case ROLLEDBACK:
										return;
								default:
										shouldContinue = state.compareAndSet(currState,State.ROLLEDBACK);
						}
				}while(shouldContinue);
		}

		@Override public boolean allowsWrites() { return false; }

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				assert state.get()==State.ACTIVE: "Cannot elevate an inactive transaction!";
				if(txnId==-1l && (parentTxn!=null && !ROOT_TRANSACTION.equals(parentTxn))){
						/*
						 * We are a read-only child transaction of a parent. This means that we didn't actually
						 * create a child transaction id or a begin timestamp of our own. Instead of elevating,
						 * we actually create a writable child transaction.
						 */
						return tc.beginChildTransaction(parentTxn,isolationLevel,dependent,additive,writeTable);
				}else{
						return tc.elevateTransaction(this, writeTable); //requires at least one network call
				}
		}
}
