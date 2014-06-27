package com.splicemachine.si.impl;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Represents a Transaction.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class WritableTxn extends AbstractTxn {

		private final Txn parent;
		private volatile long commitTimestamp = -1l;
		private volatile long globalCommitTimestamp = -1l;

		private final TxnLifecycleManager tc;

		private final boolean isDependent;
		private final boolean isAdditive;

		private volatile State state = State.ACTIVE;

		private Set<byte[]> tableWrites = new CopyOnWriteArraySet<byte[]>();


		public WritableTxn(long txnId,
											 long beginTimestamp,
											 IsolationLevel isolationLevel,
											 Txn parent,
											 TxnLifecycleManager tc,
											 boolean isDependent,
											 boolean isAdditive) {
			this(txnId, beginTimestamp, isolationLevel, parent, tc, isDependent, isAdditive,null);
		}
		public WritableTxn(long txnId,
											 long beginTimestamp,
											 IsolationLevel isolationLevel,
											 Txn parent,
											 TxnLifecycleManager tc,
											 boolean isDependent,
											 boolean isAdditive,
											 byte[] destinationTable) {
				super(txnId, beginTimestamp, isolationLevel);
				this.parent = parent;
				this.tc = tc;
				this.isDependent = isDependent;
				this.isAdditive = isAdditive;

				if(isDependent)
						assert parent!=null: "Cannot be dependent with no parent transaction";
				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

		public WritableTxn(Txn txn,TxnLifecycleManager tc, byte[] destinationTable) {
				super(txn.getTxnId(),txn.getBeginTimestamp(),txn.getIsolationLevel());
				this.parent = txn.getParentTransaction();
				this.tc = tc;
				this.isDependent = txn.isDependent();
				this.isAdditive = txn.isAdditive();
				if(isDependent)
						assert parent!=null: "Cannot be dependent with no parent transaction";
				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

		@Override public boolean isDependent() { return isDependent; }
		@Override public boolean isAdditive() { return isAdditive; }

		@Override
		public long getGlobalCommitTimestamp() {
				if(globalCommitTimestamp<0) return parent.getGlobalCommitTimestamp();
				return globalCommitTimestamp;
		}

		@Override
		public long getCommitTimestamp() {
				return commitTimestamp;
		}

		@Override
		public long getEffectiveCommitTimestamp() {
				if(globalCommitTimestamp>=0) return globalCommitTimestamp;
				if(isDependent){
						globalCommitTimestamp = parent.getEffectiveCommitTimestamp();
						return globalCommitTimestamp;
				}
				else return commitTimestamp;
		}

		@Override
		public Txn getParentTransaction() {
				return parent;
		}

		@Override
		public State getState() {
				return state;
		}

		@Override
		public void commit() throws IOException {
				switch (state){
						case COMMITTED:
								return;
						case ROLLEDBACK:
								throw new CannotCommitException(txnId,state);
				}
				synchronized (this){
						//double-checked locking for efficiency--usually not needed
						switch (state){
								case COMMITTED:
										return;
								case ROLLEDBACK:
										throw new CannotCommitException(txnId,state);
						}
						tc.commit(txnId);
						state = State.COMMITTED;
				}
		}

		@Override
		public void rollback() throws IOException {
				switch(state){
						case COMMITTED://don't need to rollback
						case ROLLEDBACK:
								return;
				}

				synchronized (this){
						switch(state){
								case COMMITTED://don't need to rollback
								case ROLLEDBACK:
										return;
						}

						tc.rollback(txnId);
						state = State.ROLLEDBACK;
				}
		}

		@Override public boolean allowsWrites() { return true; }

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				if(tableWrites.add(writeTable)){
						tc.elevateTransaction(this,writeTable);
				}
				return this;
		}

		@Override
		public Collection<byte[]> getDestinationTables() {
				return tableWrites;
		}

		public byte[] toBytes() {
				int fields = 9 + tableWrites.size();
				if(!tableWrites.isEmpty())
						fields++; //add an entry for the length
				MultiFieldEncoder encoder = MultiFieldEncoder.create(fields);
				encoder.encodeNext(getBeginTimestamp());
				Txn parentTxn = getParentTransaction();
				if(parentTxn!=null && !ROOT_TRANSACTION.equals(parentTxn))
						encoder.encodeNext(parentTxn.getTxnId());
				else
						encoder.encodeEmpty();

				encoder.encodeNext(isDependent())
								.encodeNext(allowsWrites())
								.encodeNext(isAdditive())
								.encodeNext(getIsolationLevel().getLevel())
								.encodeNext(getCommitTimestamp());
				long effectiveCommitTimestamp = getEffectiveCommitTimestamp();
				if(effectiveCommitTimestamp>0)
						encoder.encodeNext(effectiveCommitTimestamp);
				else
						encoder.encodeEmpty();
				if(!isDependent())
						encoder.encodeNext(getCommitTimestamp()); //global commit timestamp
				else
						encoder.encodeEmpty();

				//encode any destination tables
				if(tableWrites.size()>0){
						encoder.encodeNext(tableWrites.size());
						for(byte[] bytes:tableWrites){
								encoder.encodeNextUnsorted(bytes);
						}
				}

				return encoder.build();
		}
}
