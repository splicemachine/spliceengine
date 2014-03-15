package com.splicemachine.hbase.batch;

import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.api.ConstraintChecker;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/14/14
 */
public class UniqueConstraintChecker implements BatchConstraintChecker {
		private final boolean isPrimaryKey;

		private final ConstraintContext constraintContext;

		public UniqueConstraintChecker(boolean isPrimaryKey, ConstraintContext constraintContext) {
				this.isPrimaryKey = isPrimaryKey;
				this.constraintContext = constraintContext;
		}

		@Override
		public OperationStatus checkConstraint(Result result) throws IOException {
				return new OperationStatus(HConstants.OperationStatusCode.FAILURE,isPrimaryKey? "PrimaryKey":"UniqueConstraint");
		}

		@Override
		public WriteResult asWriteResult(OperationStatus opStatus){
				return new WriteResult(isPrimaryKey? WriteResult.Code.PRIMARY_KEY_VIOLATION: WriteResult.Code.UNIQUE_VIOLATION,constraintContext);
		}

		@Override
		public boolean matches(OperationStatus status){
				if(isPrimaryKey)
						return "PrimaryKey".equals(status.getExceptionMsg());
				else
						return "UniqueConstraint".equals(status.getExceptionMsg());
		}
}
