package com.splicemachine.constants;

public enum TransactionStatus {
	PENDING,
	PREPARE_COMMIT,
	DO_COMMIT,
	ABORT,
	COMMITTED,
	ABORTED
}