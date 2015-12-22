package com.splicemachine.pipeline.constraint;

import com.splicemachine.si.api.data.OperationStatusFactory;

/**
 * Indicates a Primary Key Constraint.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKeyConstraint extends UniqueConstraint {


    public PrimaryKeyConstraint(ConstraintContext cc,OperationStatusFactory osf) {
        super(cc,osf);
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new UniqueConstraintChecker(true, getConstraintContext(),this.opStatusFactory);
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
