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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.pipeline.writehandler.ConstraintWriteHandler;

public class ConstraintFactory {

    private final Constraint localConstraint;
    private PipelineExceptionFactory pipelineExceptionFactory;

    public ConstraintFactory(Constraint localConstraint,PipelineExceptionFactory pipelineExceptionFactory) {
        this.localConstraint = localConstraint;
        this.pipelineExceptionFactory=pipelineExceptionFactory;
    }

    public WriteHandler create(int expectedWrites) {
        return new ConstraintWriteHandler(localConstraint, expectedWrites,pipelineExceptionFactory);
    }

    public BatchConstraintChecker getConstraintChecker() {
        return localConstraint.asChecker();
    }

    public Constraint getLocalConstraint() {
        return localConstraint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConstraintFactory)) return false;

        ConstraintFactory that = (ConstraintFactory) o;

        return localConstraint.equals(that.localConstraint);
    }

    @Override
    public int hashCode() {
        return localConstraint.hashCode();
    }

    @Override
    public String toString() {
        return "ConstraintFactory{localConstraint=" + localConstraint + "}";
    }
}
