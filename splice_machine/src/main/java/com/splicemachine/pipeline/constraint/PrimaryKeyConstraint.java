package com.splicemachine.pipeline.constraint;

/**
 * Indicates a Primary Key Constraint.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKeyConstraint extends UniqueConstraint {

    public PrimaryKeyConstraint(ConstraintContext cc) {
        super(cc);
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new UniqueConstraintChecker(true, getConstraintContext());
    }

    @Override
    public Type getType() {
        return Type.PRIMARY_KEY;
    }

    @Override
    public String toString() {
        return "PrimaryKey";
    }
}
