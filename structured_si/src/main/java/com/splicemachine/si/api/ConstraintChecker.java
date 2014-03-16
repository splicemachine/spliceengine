package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * Hook for external systems to apply constraint checking based on rows returned during conflict detection.
 *
 * This serves multiple purposes:
 *
 * 1. Efficiency: A row only needs to be fetched a single time, and is then passed through Conflict detection
 * AND constraint checks. This minimizes the amount of IO needed to perform a constraint check.
 * 2. Correctness: Due to the concurrency of the write pipeline, some constraints are only correct if they are
 * applied synchronously (that is, within the HBase row lock). This mechanism guarantees that there is a mechanism
 * to properly apply that constraint.
 *
 * @author Scott Fines
 * Date: 3/14/14
 */
public interface ConstraintChecker {

		/**
		 * Checks the constraint against the current row and the current modification.
		 *
		 * The {@code existingRow} entity is guaranteed to be a row which exists (e.g. there are KeyValues there) and
		 * is visible to the transaction that this constraint is to be applied within.
		 *
		 * For example, suppose you are checking a constraint on row A with transaction t.
		 *
		 * If row A does not exist yet in HBase, this method will not be called and the constraint will not be checked.
		 *
		 * If row A already exists, but was written with a conflicting transaction, this method will not be checked
		 * (because a Write/Write conflict will be thrown).
		 *
		 * If row A exists, but the row is not visible to the current transaction (i.e. it was rolled back), then
		 * this method will not be checked.
		 *
		 * If row A exists, and the row is visible to transaction t, this method will be called.
		 *
		 * @param mutation the attempted write row
		 * @param existingRow the row which exists, and which is visible to the write transaction
		 * @return a Status entity representing the Constraint's conclusion
		 * @throws IOException
		 */
		public OperationStatus checkConstraint(KVPair mutation, Result existingRow) throws IOException;
}
