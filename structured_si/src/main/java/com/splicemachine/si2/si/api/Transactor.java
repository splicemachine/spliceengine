package com.splicemachine.si2.si.api;

import com.splicemachine.si2.relations.api.TuplePut;

import java.util.List;

/**
 * The primary interface to the transaction module.
 */
public interface Transactor {
	public TransactionId beginTransaction();
	public void commitTransaction(TransactionId transactionId);
	public void abortTransaction(TransactionId transactionId);
	public void failTransaction(TransactionId transactionId);

	List<TuplePut> processTuplePuts(TransactionId transactionId, List<TuplePut> tuples);
	public void filterTuple(TransactionId transactionId, Object tuple);
}
