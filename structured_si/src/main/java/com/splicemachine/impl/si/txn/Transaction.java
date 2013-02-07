package com.splicemachine.impl.si.txn;

import org.apache.hadoop.hbase.client.Attributes;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.iapi.txn.TransactionState;
import com.splicemachine.si.hbase.SIGet;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class Transaction {
	private static Logger LOG = Logger.getLogger(SIGet.class);
    protected String transactionID;
    protected long transactionStartTimestamp;
    protected long transactionStopTimestamp;
    protected TransactionState transactionState;

    public Transaction() {
    	super();
    }

    public String getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(String transactionID) {
		this.transactionID = transactionID;
	}
	
	public long getTransactionStartTimestamp() {
		return transactionStartTimestamp;
	}

	public void setTransactionStartTimestamp(long transactionStartTimestamp) {
		this.transactionStartTimestamp = transactionStartTimestamp;
	}

	public long getTransactionStopTimestamp() {
		return transactionStopTimestamp;
	}

	public void setTransactionStopTimestamp(long transactionStopTimestamp) {
		this.transactionStopTimestamp = transactionStopTimestamp;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void setTransactionState(TransactionState transactionState) {
		this.transactionState = transactionState;
	}

	public Attributes setTransactionIdToAction(Attributes attributableOperation) {
		attributableOperation.setAttribute(TxnConstants.TRANSACTION_ID, Bytes.toBytes(transactionID));
		return attributableOperation;
	}

	@Override
    public String toString() {
        return String.format("transactionID: %s, transactionStartTimeStamp %d, " +
        		"transactionCommitTimestamp %d, transactionStatus %s",transactionID,
        		transactionStartTimestamp,transactionStopTimestamp,transactionState);	
	}
	
	public static Transaction beginTransaction() {
		SpliceLogUtils.trace(LOG, "beginTransaction");
		Transaction transaction = new Transaction();
		transaction.transactionID = SIUtils.getUniqueTransactionID();
		SpliceLogUtils.trace(LOG, "transaction started %s",transaction);		
		return transaction;
	}

	public static int prepareCommit(Transaction transaction) {
		SpliceLogUtils.trace(LOG, "prepareCommit %s",transaction);
		return 0;
	}

	public static void doCommit(Transaction transaction) {
		SpliceLogUtils.trace(LOG, "doCommit %s",transaction);
	}
	
	public static void abort(Transaction transaction) {
		SpliceLogUtils.trace(LOG, "doCommit %s",transaction);
	}
	

}
