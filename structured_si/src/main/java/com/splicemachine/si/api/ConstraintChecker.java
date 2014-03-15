package com.splicemachine.si.api;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public interface ConstraintChecker {

		/**
		 * Guaranteed to be visible to the current transaction, and also to occur INSIDE the
		 * HBase row lock.
		 *
		 * @param result
		 * @return
		 * @throws Exception
		 */
		public OperationStatus checkConstraint(Result result) throws IOException;

		public OperationStatus lockTimedOut();
}
