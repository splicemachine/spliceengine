package com.splicemachine.si.hbase;

public class SITransaction {
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
