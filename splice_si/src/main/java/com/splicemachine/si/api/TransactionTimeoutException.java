package com.splicemachine.si.api;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a Transaction has timed out,
 * and a specific action cannot be taken
 * @author Scott Fines
 * Date: 6/25/14
 */
public class TransactionTimeoutException  extends DoNotRetryIOException{
		public TransactionTimeoutException() {
		}


		public TransactionTimeoutException(long txnId) {
				super("Transaction "+ txnId+" has timed out");
		}

		public TransactionTimeoutException(String message) {
				super(message);
		}

		public TransactionTimeoutException(String message, Throwable cause) {
				super(message, cause);
		}
}
