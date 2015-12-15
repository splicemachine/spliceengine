package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.server.ConstraintChecker;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public interface BatchConstraintChecker<OperationStatus> extends ConstraintChecker {
    WriteResult asWriteResult(OperationStatus status);
    boolean matches(OperationStatus status);
}
