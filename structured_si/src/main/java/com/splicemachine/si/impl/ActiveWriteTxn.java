package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

/**
 * @author Scott Fines
 *         Date: 7/3/14
 */
public class ActiveWriteTxn extends AbstractTxnView{
		private final TxnView parentTxn;
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
														 TxnView parentTxn,
                             boolean additive) {
				super(txnId, beginTimestamp, Txn.IsolationLevel.SNAPSHOT_ISOLATION);
				this.parentTxn = parentTxn;
        this.additive = additive;
		}


		@Override public long getCommitTimestamp() { return -1l; }
		@Override public long getEffectiveCommitTimestamp() { return -1l; }
    @Override public TxnView getParentTxnView() { return parentTxn; }

    @Override public Txn.State getState() { return Txn.State.ACTIVE; }

		@Override public boolean allowsWrites() { return true; }

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
