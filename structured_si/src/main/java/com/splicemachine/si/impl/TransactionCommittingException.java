package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Used to indicate that a Transaction is stuck in the COMMITTING phase,
 * and is unable to escape within the allotted wait time.
 *
 * @author Scott Fines
 * Date: 1/29/14
 */
public class TransactionCommittingException extends DoNotRetryIOException{
		public TransactionCommittingException() { }
		public TransactionCommittingException(long transactionId) {
				super("Transaction "+ transactionId+" is committing");
		}
		public TransactionCommittingException(String message) { super(message); }
		public TransactionCommittingException(String message, Throwable cause) { super(message, cause); }
}
