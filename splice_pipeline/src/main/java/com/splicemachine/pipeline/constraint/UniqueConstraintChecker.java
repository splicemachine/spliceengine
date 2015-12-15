package com.splicemachine.pipeline.constraint;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.BatchConstraintChecker;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public class UniqueConstraintChecker implements BatchConstraintChecker {
    private OperationStatusFactory statusLib = SIDriver.getOperationStatusLib();
    private final MutationStatus SUCCESS = statusLib.success();

    private final WriteResult result;
    private final boolean isPrimaryKey;
//    private final OperationStatus failure;

    private MutationStatus failure;


    public UniqueConstraintChecker(boolean isPrimaryKey, ConstraintContext constraintContext) {
        this.isPrimaryKey = isPrimaryKey;
        this.result = new WriteResult(isPrimaryKey ? Code.PRIMARY_KEY_VIOLATION : Code.UNIQUE_VIOLATION, constraintContext);
//        this.failure = new OperationStatus(HConstants.OperationStatusCode.FAILURE, isPrimaryKey ? "PrimaryKey" : "UniqueConstraint");
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {
        // There is an existing row, if this is an insert then fail.
        return mutation.getType() == KVPair.Type.INSERT ? failure : SUCCESS;
    }

    @Override
    public WriteResult asWriteResult(MutationStatus opStatus) {
        return result;
    }

    @Override
    public boolean matches(MutationStatus status) {
        if (isPrimaryKey)
            return "PrimaryKey".equals(status.getExceptionMsg());
        else
            return "UniqueConstraint".equals(status.getExceptionMsg());
    }
}
