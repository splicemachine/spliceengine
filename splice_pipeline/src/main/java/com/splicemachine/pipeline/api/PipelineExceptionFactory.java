package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.constraint.ConstraintContext;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public interface PipelineExceptionFactory{
    IOException primaryKeyViolation(ConstraintContext constraintContext);
}
