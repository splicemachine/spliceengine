package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a Transaction has timed out,
 * and a specific action cannot be taken
 * @author Scott Fines
 * Date: 6/25/14
 */
public class HTransactionTimeout extends DoNotRetryIOException implements TransactionTimeoutException{
		public HTransactionTimeout() {
		}
		public HTransactionTimeout(long txnId) {
				super("Transaction "+ txnId+" has timed out");
		}

		public HTransactionTimeout(String message) {
				super(message);
		}

		public HTransactionTimeout(String message,Throwable cause) {
				super(message, cause);
		}
}
