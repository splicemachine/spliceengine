package com.splicemachine.pipeline.constraint;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.WriteResult;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/14/14
 */
public class UniqueConstraintChecker implements BatchConstraintChecker {
		private static final OperationStatus SUCCESS = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
		private final WriteResult result;
		private final boolean isPrimaryKey;


		public UniqueConstraintChecker(boolean isPrimaryKey, ConstraintContext constraintContext) {
				this.isPrimaryKey = isPrimaryKey;
				this.result =  new WriteResult(isPrimaryKey? Code.PRIMARY_KEY_VIOLATION: Code.UNIQUE_VIOLATION,constraintContext);
		}

		@Override
		public OperationStatus checkConstraint(KVPair mutation, Result result) throws IOException {
				//unique constraint checking only applies for INSERT operations
				if(mutation.getType()== KVPair.Type.INSERT)
						return new OperationStatus(HConstants.OperationStatusCode.FAILURE,isPrimaryKey? "PrimaryKey":"UniqueConstraint");
				return SUCCESS;
		}

		@Override
		public WriteResult asWriteResult(OperationStatus opStatus){
				return result;
		}

		@Override
		public boolean matches(OperationStatus status){
				if(isPrimaryKey)
						return "PrimaryKey".equals(status.getExceptionMsg());
				else
						return "UniqueConstraint".equals(status.getExceptionMsg());
		}
}
