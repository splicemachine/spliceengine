package com.splicemachine.pipeline.constraint;

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.exception.ErrorState;

/**
 * Utilities relating to Constraints
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class Constraints {

    private Constraints() {
    } //can't make me!

    public static Exception constraintViolation(Code writeErrorCode, ConstraintContext constraintContext) {
        switch (writeErrorCode) {
            case UNIQUE_VIOLATION:
            case PRIMARY_KEY_VIOLATION:
                return ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.newException(constraintContext.getMessages());
            case FOREIGN_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, constraintContext);
            case CHECK_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.CHECK, constraintContext);
        }
        return null;
    }
}
