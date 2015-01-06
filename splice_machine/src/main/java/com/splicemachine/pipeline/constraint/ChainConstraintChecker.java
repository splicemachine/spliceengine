package com.splicemachine.pipeline.constraint;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.ConstraintChecker;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public class ChainConstraintChecker implements BatchConstraintChecker {
    private List<BatchConstraintChecker> delegates;

    public ChainConstraintChecker(List<BatchConstraintChecker> delegates) {
        this.delegates = delegates;
    }

    @Override
    public OperationStatus checkConstraint(KVPair mutation, Result existingRow) throws IOException {
        HConstants.OperationStatusCode code = HConstants.OperationStatusCode.SUCCESS;
        OperationStatus status = null;
        for (ConstraintChecker delegate : delegates) {
            status = delegate.checkConstraint(mutation, existingRow);
            if (status != null)
                code = status.getOperationStatusCode();
            if (code != HConstants.OperationStatusCode.SUCCESS)
                return status;
        }
        return status;
    }

    @Override
    public WriteResult asWriteResult(OperationStatus status) {
        for (BatchConstraintChecker checker : delegates) {
            if (checker.matches(status))
                return checker.asWriteResult(status);
        }
        return null;
    }

    @Override
    public boolean matches(OperationStatus status) {
        for (BatchConstraintChecker checker : delegates) {
            if (checker.matches(status))
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ChainConstraintChecker{" +
                "delegates=" + (delegates == null ? 0 : delegates.size()) +
                '}';
    }
}
