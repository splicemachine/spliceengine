package com.splicemachine.pipeline.constraint;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.BatchConstraintChecker;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.data.SOperationStatusLib;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public class UniqueConstraintChecker<OperationStatus> implements BatchConstraintChecker<OperationStatus> {
    private SOperationStatusLib<OperationStatus> statusLib = SIDriver.getOperationStatusLib();
    private final OperationStatus SUCCESS = statusLib.getSuccess();

    private final WriteResult result;
    private final boolean isPrimaryKey;
    private final OperationStatus failure;


    public UniqueConstraintChecker(boolean isPrimaryKey, ConstraintContext constraintContext) {
        this.isPrimaryKey = isPrimaryKey;
        this.result = new WriteResult(isPrimaryKey ? Code.PRIMARY_KEY_VIOLATION : Code.UNIQUE_VIOLATION, constraintContext);
        this.failure = statusLib.g
        this.failure = new OperationStatus(HConstants.OperationStatusCode.FAILURE, isPrimaryKey ? "PrimaryKey" : "UniqueConstraint");
    }

    @Override
    public OperationStatus checkConstraint(KVPair mutation, Result existingRow) throws IOException {
        // There is an existing row, if this is an insert then fail.
        return mutation.getType() == KVPair.Type.INSERT ? failure : SUCCESS;
    }

    @Override
    public WriteResult asWriteResult(OperationStatus opStatus) {
        return result;
    }

    @Override
    public boolean matches(OperationStatus status) {
        if (isPrimaryKey)
            return "PrimaryKey".equals(status.getExceptionMsg());
        else
            return "UniqueConstraint".equals(status.getExceptionMsg());
    }
}
