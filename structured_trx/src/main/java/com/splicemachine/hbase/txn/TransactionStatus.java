package com.splicemachine.hbase.txn;

public enum TransactionStatus {
	PENDING,
	PREPARE_COMMIT,
	DO_COMMIT,
	ABORT,
	COMMITTED,
	ABORTED
}