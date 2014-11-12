package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.utils.ByteSlice;

public class HTransactionLib implements STransactionLib<SparseTxn,ByteSlice> {

	@Override
	public long getTxnId(SparseTxn transaction) {
		return transaction.getTxnId();
	}

	@Override
	public State getTransactionState(SparseTxn transaction) {
		return transaction.getState();
	}

	@Override
	public long getParentTxnId(SparseTxn transaction) {
		return transaction.getParentTxnId();
	}

	@Override
	public long getBeginTimestamp(SparseTxn transaction) {
		return transaction.getBeginTimestamp();
	}

	@Override
	public boolean hasAddiditiveField(SparseTxn transaction) {
		return transaction.hasAdditiveField();
	}

	@Override
	public boolean isAddiditive(SparseTxn transaction) {
		return transaction.isAdditive();
	}

	@Override
	public long getCommitTimestamp(SparseTxn transaction) {
		return transaction.getCommitTimestamp();
	}

	@Override
	public long getGlobalCommitTimestamp(SparseTxn transaction) {
		return transaction.getGlobalCommitTimestamp();
	}

	@Override
	public boolean isTimedOut(SparseTxn transaction) {
		return transaction.isTimedOut();
	}

	@Override
	public IsolationLevel getIsolationLevel(SparseTxn transaction) {
		return transaction.getIsolationLevel();
	}

	@Override
	public TxnDecoder getV1TxnDecoder() {
		return V1TxnDecoder.INSTANCE;
	}

	@Override
	public TxnDecoder getV2TxnDecoder() {
		return V2TxnDecoder.INSTANCE;
	}

	@Override
	public ByteSlice getDestinationTableBuffer(SparseTxn transaction) {
		return transaction.getDestinationTableBuffer();
	}


}
