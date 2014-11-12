package com.splicemachine.pipeline.constraint;

import org.apache.log4j.Logger;

/**
 * Indicates a Primary Key Constraint.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class PrimaryKey extends UniqueConstraint {
    private static final Logger logger = Logger.getLogger(PrimaryKey.class);

    public PrimaryKey(ConstraintContext cc){
        super(cc);
    }

		@Override
		public BatchConstraintChecker asChecker() {
				return new UniqueConstraintChecker(true,getConstraintContext());
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
