package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 7/3/14
 */
public class ActiveWriteTxn extends AbstractTxn{
		private final Txn parentTxn;
    private final boolean additive;

    public ActiveWriteTxn(long txnId,
                          long beginTimestamp){
        this(txnId,beginTimestamp,Txn.ROOT_TRANSACTION);
    }

    public ActiveWriteTxn(long txnId,
                             long beginTimestamp,
                             Txn parentTxn){
        this(txnId,beginTimestamp,parentTxn,false);
    }

		public ActiveWriteTxn(long txnId,
														 long beginTimestamp,
														 Txn parentTxn,
                             boolean additive) {
				super(txnId, beginTimestamp,IsolationLevel.SNAPSHOT_ISOLATION);
				this.parentTxn = parentTxn;
        this.additive = additive;
		}


		@Override public long getCommitTimestamp() { return -1l; }
		@Override public long getEffectiveCommitTimestamp() { return -1l; }
		@Override public Txn getParentTransaction() { return parentTxn; }
		@Override public State getState() { return State.ACTIVE; }

		@Override
		public void commit() throws IOException {
			throw new UnsupportedOperationException("Cannot commit an ActiveWriteTxn");
		}

		@Override
		public void rollback() throws IOException {
			throw new UnsupportedOperationException("Cannot rollback an ActiveWriteTxn");
		}

		@Override public boolean allowsWrites() { return true; }

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				throw new UnsupportedOperationException("Cannot elevate an ActiveWriteTxn");
		}

		@Override public boolean isDependent() { return false; }
		@Override public boolean isAdditive() { return additive; }
		@Override public long getGlobalCommitTimestamp() { return -1l; }

    @Override
    public String toString() {
        if(parentTxn!=null)
            return "ActiveWriteTxn("+txnId+","+parentTxn.getTxnId()+")";
        else
            return "ActiveWriteTxn("+txnId+","+"-1)";
    }
}
