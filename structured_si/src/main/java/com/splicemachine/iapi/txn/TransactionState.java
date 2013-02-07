package com.splicemachine.iapi.txn;
/**
 * 
 * Transaction State for transactions
 * 
 * Active: Transaction Has Begun
 * Error: Error during processing
 * Commit: In process of committing transaction
 * Abort: Rolling back transaction
 * Complete: Transaction Has Completed
 * 
 * @author John Leach
 *
 */
public enum TransactionState {
	ACTIVE,ERROR,COMMIT,ABORT,COMPLETE
}
