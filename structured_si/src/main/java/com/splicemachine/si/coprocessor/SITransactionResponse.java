package com.splicemachine.si.coprocessor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.iapi.txn.TransactionState;

public class SITransactionResponse implements Externalizable {
	protected long startTimestamp = 0;
	protected long commitTimestamp = 0;
	protected TransactionState transactionState;
	public SITransactionResponse() {
		super();
	}
	
	public SITransactionResponse (long startTimestamp, long commitTimestamp, TransactionState transactionState) {
		this.startTimestamp = startTimestamp;
		this.commitTimestamp = commitTimestamp;
		this.transactionState = transactionState;
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		startTimestamp = in.readLong();
		commitTimestamp = in.readLong();
		transactionState = TransactionState.values()[in.readInt()];
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(startTimestamp);
		out.writeLong(commitTimestamp);
		out.writeInt(transactionState.ordinal());
	}

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

	public long getCommitTimestamp() {
		return commitTimestamp;
	}

	public void setCommitTimestamp(long commitTimestamp) {
		this.commitTimestamp = commitTimestamp;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void setTransactionState(TransactionState transactionState) {
		this.transactionState = transactionState;
	}

}