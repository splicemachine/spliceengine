package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.constraint.Constraint;
import com.splicemachine.pipeline.writehandler.ConstraintWriteHandler;

class ConstraintFactory {

    private final Constraint localConstraint;

    ConstraintFactory(Constraint localConstraint) {
        this.localConstraint = localConstraint;
    }

    public WriteHandler create() {
        return new ConstraintWriteHandler(localConstraint);
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
}
