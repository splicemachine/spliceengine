package com.splicemachine.pipeline.constraint;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

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
    public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {
        MutationStatus status = null;
        for (ConstraintChecker delegate : delegates) {
            status = delegate.checkConstraint(mutation, existingRow);
            if (status != null)
            if (!status.isSuccess())
                return status;
        }
        return status;
    }

    @Override
    public WriteResult asWriteResult(MutationStatus status) {
        for (BatchConstraintChecker checker : delegates) {
            if (checker.matches(status))
                return checker.asWriteResult(status);
        }
        return null;
    }

    @Override
    public boolean matches(MutationStatus status) {
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
