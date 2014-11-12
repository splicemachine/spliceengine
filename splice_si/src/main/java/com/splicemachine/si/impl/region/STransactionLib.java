package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.Txn;

public interface STransactionLib<Transaction,TableBuffer> {
	long getTxnId(Transaction transaction);
	Txn.State getTransactionState(Transaction transaction);
	long getParentTxnId(Transaction transaction);
	long getBeginTimestamp(Transaction transaction);
	boolean hasAddiditiveField(Transaction transaction);
	boolean isAddiditive(Transaction transaction);	
	long getCommitTimestamp(Transaction transaction);
	long getGlobalCommitTimestamp(Transaction transaction);
	boolean isTimedOut(Transaction transaction);
	Txn.IsolationLevel getIsolationLevel(Transaction transaction);
	TxnDecoder getV1TxnDecoder();
	TxnDecoder getV2TxnDecoder();
	TableBuffer getDestinationTableBuffer(Transaction transaction);
}
