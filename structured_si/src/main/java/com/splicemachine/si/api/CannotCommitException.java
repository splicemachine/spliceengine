package com.splicemachine.si.api;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class CannotCommitException extends DoNotRetryIOException {

		public CannotCommitException(long txnId,Txn.State actualState){
				super("Transaction "+txnId+" cannot be committed--it is in the "+ actualState+" state");
		}
}
