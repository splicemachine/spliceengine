package com.splicemachine.si.api;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/24/14
 */
public interface TxnStore extends TxnAccess{

		/**
		 * Write the Transaction to underlying storage.
		 *
		 * @param txn the transaction to write.
		 * @throws IOException if something goes wrong trying to write it
		 */
		public void recordNewTransaction(Txn txn) throws IOException;

		public void rollback(long txnId) throws IOException;

		public long commit(long txnId) throws IOException;

		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException;

		/**
		 * Get a list of active write transactions. Only transactions which write
		 * are guaranteed to be returned (although some implementations may opt to return
		 * transactions which are read-only if they so desire).
		 *
		 * @param txn the transaction with the maximum id to return. Only transactions
		 *            which have a transaction id <= {@code txn.getTxnId()} will be returned. If
		 *            {@code txn ==null}, then all write transactions will be returned.
		 * @param table the table to limit, or {@code null} if all write transactions are to be
		 *              returned. If the table is not null, then only transactions which are affecting
		 *              the specified table will be returned.
		 * @return all write transaction ids (optionally, some read-only transactions as well) which
		 * 				are <= {@code txn.getTxnId()}. If {@code txn} is null, then all write transactions
		 * 			will be returned.
		 * @throws IOException
		 */
		long[] getActiveTransactions(Txn txn,byte[] table) throws IOException;

		long[] getActiveTransactions(long minTxnId,long maxTxnId,byte[] table) throws IOException;

}
