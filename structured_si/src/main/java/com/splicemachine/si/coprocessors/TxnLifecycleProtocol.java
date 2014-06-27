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
		void beginTransaction(long txnId,byte[] packedTxn) throws IOException;

		/**
		 * Elevate a top-level transaction from read-only to writable.
		 *
		 * @param txnId the unique id of the transaction. This is a convenience in order to construct and parse row keys.
		 * @param newDestinationTable a destination table that this transaction is writing to.
		 * @throws IOException if something goes wrong when elevating the transaction
		 */
		void elevateTransaction(long txnId, byte[] newDestinationTable) throws IOException;

		/**
		 * Create a <em>writable</em> child transaction.
		 *
		 * Because the Transaction table consists of multiple regions spread across multiple nodes, the semantics
		 * of this call depend greatly on what row this is called against(in particular, which region it's called against).
		 * In particular, if this method is called against the <em>parent transaction's</em> row, then
		 * <em>creating</em> the child transaction record will incur a remote call--as a result, this should only be done
		 * when the child transaction id is to be generated.
		 *
		 * When the child transaction id(and begin timestamp) are known <em>before</em> the network call, then this
		 * method may be called against the child transaction's region, which will incur a possible network <em>read</em>
		 * of the parent transaction information if additional information is needed (i.e. if it inherits from
		 * the parent
		 *
		 * @param parentTxnId the id of the parent transaction
		 * @param packedChildTxn a packed representation of the child transaction to be created. This may be a partially
		 *                       constructed transaction--in particular, the call to
		 *                       {@link com.splicemachine.si.api.Txn#getIsolationLevel()} may be {@code null},
		 *                       and {@link com.splicemachine.si.api.Txn#getBeginTimestamp()} may == -1. If
		 *                       {@code Txn#getBeginTimestamp()==-1}, then {@link com.splicemachine.si.api.Txn#getTxnId()}==-1
		 *                       as well, or an assertion error may be thrown (as the transaction id is invalid without
		 *                       a begin timestamp)
		 * @return a packed byte-representation of the child transaction, with fully constructed fields.
		 */
		byte[] beginChildTransaction(long parentTxnId, byte[] packedChildTxn) throws IOException;

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

		byte[] getTransaction(long txnId) throws IOException;
}
