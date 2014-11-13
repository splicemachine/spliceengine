package com.splicemachine.si.impl.region;

import com.google.protobuf.ByteString;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.coprocessor.TxnMessage;

public class HTransactionLib implements STransactionLib<TxnMessage.Txn,ByteString> {

	@Override
	public long getTxnId(TxnMessage.Txn transaction) {
		return transaction.getInfo().getTxnId();
	}

	@Override
	public State getTransactionState(TxnMessage.Txn transaction) {
		return State.fromInt(transaction.getState());
	}

	@Override
	public long getParentTxnId(TxnMessage.Txn transaction) {
		return transaction.getInfo().getParentTxnid();
	}

	@Override
	public long getBeginTimestamp(TxnMessage.Txn transaction) {
		return transaction.getInfo().getBeginTs();
	}

	@Override
	public boolean hasAddiditiveField(TxnMessage.Txn transaction) {
		return transaction.getInfo().hasIsAdditive();
	}

	@Override
	public boolean isAddiditive(TxnMessage.Txn transaction) {
		return transaction.getInfo().hasIsAdditive();
	}

	@Override
	public long getCommitTimestamp(TxnMessage.Txn transaction) {
		return transaction.getCommitTs();
	}

	@Override
	public long getGlobalCommitTimestamp(TxnMessage.Txn transaction) {
		return transaction.getGlobalCommitTs();
	}

	@Override
	public boolean isTimedOut(TxnMessage.Txn transaction) {
		return Txn.State.fromInt(transaction.getState()).equals(Txn.State.ROLLEDBACK);
	}

	@Override
	public IsolationLevel getIsolationLevel(TxnMessage.Txn transaction) {
		return IsolationLevel.fromInt(transaction.getInfo().getIsolationLevel());
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
	public ByteString getDestinationTableBuffer(TxnMessage.Txn transaction) {
		return transaction.getInfo().getDestinationTables();
	}


}
