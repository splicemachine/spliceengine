package com.splicemachine.si.coprocessors;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * Represents a network protocol for managing a Transaction's lifecycle.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public interface TxnLifecycleProtocol extends CoprocessorProtocol {

		/**
		 * Begin a top-level <em>writable</em> transaction. The returned transaction will be
		 * recorded on the transaction table and will be in the ACTIVE state (able to be committed,rolled back etc.)
		 *
		 * @param txnId the unique id of the transaction. This is a convenience in order to construct and parse row keys.
		 * @param packedTxn a packed representation of the transaction, with all empty fields placed
		 *                  with appropriate defaults. Note that the transaction id and begin timestamp
		 *                  <em>must</em> be present.
		 * @throws IOException if something goes wrong in creating the transaction
		 */
		void recordTransaction(long txnId, byte[] packedTxn) throws IOException;

		/**
		 * Elevate a top-level transaction from read-only to writable.
		 *
		 * @param txnId the unique id of the transaction. This is a convenience in order to construct and parse row keys.
		 * @param newDestinationTable a destination table that this transaction is writing to.
		 * @throws IOException if something goes wrong when elevating the transaction
		 */
		void elevateTransaction(long txnId, byte[] newDestinationTable) throws IOException;

		/**
		 * Commit the transaction.
		 *
		 * If the transaction is read-only, then no Transaction record is required to exist. In this case, a call
		 * to this method will perform a lookup to find the transaction, but otherwise will perform no activities.
		 *
		 * If the transaction has been rolled back or timed out, this will throw an exception.
		 *
		 * If the transaction has already been committed, this will perform no action, and will return the previously
		 * created transaction id.
		 *
		 * If this transaction is a dependent child transaction, a commit timestamp will still be generated for this
		 * transaction and returned.
		 *
		 * This call is atomic: it either wholly succeeds or wholly fails. If it succeeds, then the transaction
		 * is considered committed; otherwise, it's left in its previous state. However, this does not prevent
		 * network actions from causing difficulty--If the network fails while returning data, the transaction may
		 * have been committed. However, in this case retrying the operation is acceptable, because committing a second
		 * time performs no actions.
		 *
		 * @param txnId the transaction id to commit
		 * @throws IOException if something goes wrong.
		 * @throws com.splicemachine.si.api.CannotCommitException If the transaction cannot be committed because it has
		 * already been rolled back or timed out.
		 * @return the commit timestamp for the transaction
		 */
		long commit(long txnId) throws IOException;

		/**
		 * Roll back the transaction.
		 *
		 * If the transaction is read-only, then no transaction record is required to exist; this call may perform
		 * IO to find the missing row, but will otherwise perform no actions in this case.
		 *
		 * If the transaction has already been committed, then this call will do nothing.
		 *
		 * If the transaction has already been rolled back or timed out, then this call will do nothing.
		 *
		 * This call is atomic--it either succeeds, or the transaction is left in its prior state. However, this
		 * does not prevent network calls from failing after the rollback has completed successfully. In this case,
		 * calling this method again is safe, as it will perform no action.
		 *
		 * @param txnId the transaction id to rollback
		 * @throws IOException if something goes wrong.
		 */
		void rollback(long txnId) throws IOException;


/**
		 * Indicate that this transaction is still alive.
		 *
		 * If the transaction is in a terminal state, then this will return {@code false}, to indicate
		 * that future keep alives are not necessary.
		 *
		 * If the transaction has exceeded its keep-alive limit, this will throw a
		 * {@link com.splicemachine.si.api.TransactionTimeoutException}
		 *
		 * @param txnId the transaction to keep alive.
		 * @return true if the transaction is still active after the keep-alive has completed, or {@code false}
		 * 							if the transaction is in a terminal state and further keep-alives should not be attempted.
		 * @throws IOException if something goes wrong attempting to keep it alive.
		 */
		boolean keepAlive(long txnId) throws IOException;

		byte[] getTransaction(long txnId,boolean getDestinationTables) throws IOException;

		/**
		 * Return an estimate of the currently active transactions. If {@code destinationTable !=null},
		 * then this will return only transactions which directly report modifying a specific table.
		 *
		 * Note that this will by-pass modification locks. As such it may rely on HBase to provide accuracy
		 * guarantees. In other words, make sure that any time this is called, you are careful about the
		 * transactional identifiers to be used. Otherwise, a transaction which has been rolled back may
		 * actually be considered active with respect to this operation. However, transactions which
		 * have been successfully committed or rolled back <em>will not</em> be on this list. That is,
		 * it is possible to have false positives, but no false negatives.
		 *
		 * @param destinationTable a destination table to filter against
		 * @param beforeTs if >=0, will return only transactions whose begin timestamp occurs before this number
		 * @param afterTs if >=0, will return only transactions whose begin timestamp occurs after this number
		 * @return a list of transaction ids which satisfy the constraints, encoded using a packed encoding.
		 * @throws IOException if something goes wrong.
		 */
		byte[] getActiveTransactions(long afterTs,long beforeTs,byte[] destinationTable) throws IOException;
}
