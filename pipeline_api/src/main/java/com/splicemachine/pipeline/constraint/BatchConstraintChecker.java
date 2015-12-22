package com.splicemachine.pipeline.constraint;

import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public interface BatchConstraintChecker extends ConstraintChecker {
    WriteResult asWriteResult(MutationStatus status);
    boolean matches(MutationStatus status);
}
