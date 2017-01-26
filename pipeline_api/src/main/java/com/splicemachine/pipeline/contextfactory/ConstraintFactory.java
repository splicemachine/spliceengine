/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
