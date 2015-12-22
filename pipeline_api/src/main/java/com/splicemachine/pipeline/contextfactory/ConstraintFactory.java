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
