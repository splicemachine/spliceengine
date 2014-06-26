package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;

/**
 * Interface for notifying other systems that a read was resolved.
 *
 * This is somewhat similar to the RollForward structure, in that
 * it is allowed to asynchronously resolve data as written, but is
 * different in its expected usage. In this case, it is allowed
 * to directly modify data--it is not required to wait any period
 * for an unknown transaction state to resolve itself externally--
 * the state is known directly ahead of time.
 *
 * @author Scott Fines
 * Date: 6/25/14
 */
public interface ReadResolver {

		/**
		 * Resolve a particular row and transaction id as committed (e.g.
		 * add a commit timestamp column to the entry if needed.
		 *
		 * @param rowKey the row key to resolve
		 * @param txnId the transaction id (e.g. Hbase version) to resolve as committed
		 * @param commitTimestamp the commit timestamp to resolve with
		 */
		void resolveCommitted(ByteSlice rowKey, long txnId,long commitTimestamp);

		/**
		 * Resolve a particular row and transaction id as rolled back(e.g.
		 * delete the particular version of the data)
		 *
		 * @param rowKey the row key to resolve
		 * @param txnId the transaction id (e.g. Hbase version) to resolve as rolled back
		 */
		void resolveRolledback(ByteSlice rowKey, long txnId);
}
