package com.splicemachine.hbase.batch;

import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.ConstraintChecker;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public interface BatchConstraintChecker extends ConstraintChecker {
		public WriteResult asWriteResult(OperationStatus status);

		public boolean matches(OperationStatus status);
}
