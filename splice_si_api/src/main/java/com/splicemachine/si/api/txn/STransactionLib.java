package com.splicemachine.si.api.txn;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnDecoder;

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
	TxnDecoder getV2TxnDecoder();
	TableBuffer getDestinationTableBuffer(Transaction transaction);
}
