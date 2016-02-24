package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.data.ExceptionFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public interface PipelineExceptionFactory extends ExceptionFactory{
    IOException primaryKeyViolation(ConstraintContext constraintContext);

    IOException foreignKeyViolation(ConstraintContext constraintContext);

    IOException uniqueViolation(ConstraintContext constraintContext);

    IOException notNullViolation(ConstraintContext constraintContext);

    Throwable processPipelineException(Throwable t);

    boolean needsTransactionalRetry(Throwable t);

    boolean canFinitelyRetry(Throwable t);

    boolean canInfinitelyRetry(Throwable t);

    Throwable processErrorResult(WriteResult value);

    IOException fromErrorString(String s);
}
