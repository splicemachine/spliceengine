package com.splicemachine.impl.si.txn;

import org.apache.log4j.Logger;

import com.splicemachine.iapi.txn.TransactionManager;
import com.splicemachine.iapi.txn.TransactionState;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class Transaction {
	private static Logger LOG = Logger.getLogger(Transaction.class);
    protected long startTimestamp;
    protected long stopTimestamp;
    protected TransactionState transactionState;

    public Transaction() {
    	super();
    }

    public Transaction(long transacton) {
    	super();
    }

	public long getStartTimestamp() {
		return startTimestamp;
	}

	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

	public long getstopTimestamp() {
		return stopTimestamp;
	}

	public void setstopTimestamp(long stopTimestamp) {
		this.stopTimestamp = stopTimestamp;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void setTransactionState(TransactionState transactionState) {
		this.transactionState = transactionState;
	}

	@Override
    public String toString() {
        return String.format("transactionStartTimeStamp %d, " +
        		"transactionCommitTimestamp %d, transactionStatus %s",
        		startTimestamp,stopTimestamp,transactionState);	
	}
	
	public int prepareCommit() {
		SpliceLogUtils.trace(LOG, "prepareCommit %s",this);
		return 0;
	}

	public void doCommit() {
		SpliceLogUtils.trace(LOG, "doCommit %s",this);
	}
	
	public void abort() {
		SpliceLogUtils.trace(LOG, "doCommit %s",this);
	}
	

}
