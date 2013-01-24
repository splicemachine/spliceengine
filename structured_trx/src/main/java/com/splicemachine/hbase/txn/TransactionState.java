package com.splicemachine.hbase.txn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Attributes;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.constants.TxnConstants;

public class TransactionState {
    static final Log LOG = LogFactory.getLog(TransactionState.class);
    private String transactionID;

    public TransactionState(String transactionID) {
    	this.transactionID = transactionID;
    }

    public TransactionState(byte[] transactionByteArray) {
    	this.transactionID = Bytes.toString(transactionByteArray);
    }

    public String getTransactionID() {
		return transactionID;
	}

	public void setTransactionID(String transactionID) {
		this.transactionID = transactionID;
	}
	
	public Attributes setTransactionIdToAction(Attributes attributableOperation) {
		attributableOperation.setAttribute(TxnConstants.TRANSACTION_ID, Bytes.toBytes(transactionID));
		return attributableOperation;
	}

	@Override
    public String toString() {
        return "id: " + transactionID;
    }
}
