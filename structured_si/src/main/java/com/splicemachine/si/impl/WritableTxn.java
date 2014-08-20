package com.splicemachine.si.impl;

import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SliceIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Represents a Transaction.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class WritableTxn extends AbstractTxn {

		private final TxnView parent;
		private volatile long commitTimestamp = -1l;
		private volatile long globalCommitTimestamp = -1l;

		private final TxnLifecycleManager tc;

		private final boolean isAdditive;

		private volatile State state = State.ACTIVE;

		private Set<byte[]> tableWrites = new CopyOnWriteArraySet<byte[]>();


		public WritableTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parent,
                       TxnLifecycleManager tc,
                       boolean isAdditive) {
			this(txnId, beginTimestamp, isolationLevel, parent, tc, isAdditive,null);
		}
		public WritableTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parent,
                       TxnLifecycleManager tc,
                       boolean isAdditive,
                       byte[] destinationTable) {
				super(txnId, beginTimestamp, isolationLevel);
				this.parent = parent;
				this.tc = tc;
				this.isAdditive = isAdditive;

				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

		public WritableTxn(Txn txn,TxnLifecycleManager tc, byte[] destinationTable) {
				super(txn.getTxnId(),txn.getBeginTimestamp(),txn.getIsolationLevel());
				this.parent = txn.getParentTxnView();
				this.tc = tc;
				this.isAdditive = txn.isAdditive();
				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

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
        if(Txn.ROOT_TRANSACTION.equals(parent))
            globalCommitTimestamp = commitTimestamp;
        else
						globalCommitTimestamp = parent.getEffectiveCommitTimestamp();

        return globalCommitTimestamp;
		}

    @Override public TxnView getParentTxnView() { return parent; }

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
						commitTimestamp = tc.commit(txnId);
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
        assert state==State.ACTIVE: "Cannot elevate an inactive transaction: "+ state;
				if(tableWrites.add(writeTable)){
						tc.elevateTransaction(this,writeTable);
				}
				return this;
		}

		@Override
		public Iterator<ByteSlice> getDestinationTables() {
        return new SliceIterator(tableWrites.iterator());
		}

    @Override
    public String toString(){
        return "WritableTxn("+txnId+","+getState()+")";
    }
}
