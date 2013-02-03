package com.splicemachine.si.hbase;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

public class SITransaction {
	private static Logger LOG = Logger.getLogger(SITransaction.class);
	public enum TransactionState {BEGIN,COMMITTED,ROLLBACK,COMPLETE}
	protected String transactionID;
	protected long beginTimestamp;
	protected long commitTimestamp;
	protected TransactionState transactionState;
	public String getTransactionID() {
		return transactionID;
	}
	public void setTransactionID(String transactionID) {
		this.transactionID = transactionID;
	}
	public long getBeginTimestamp() {
		return beginTimestamp;
	}
	public void setBeginTimestamp(long beginTimestamp) {
		this.beginTimestamp = beginTimestamp;
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
