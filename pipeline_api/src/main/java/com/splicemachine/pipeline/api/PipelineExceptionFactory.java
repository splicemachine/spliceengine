/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

    Exception processErrorResult(WriteResult value);

    IOException fromErrorString(String s);
}
