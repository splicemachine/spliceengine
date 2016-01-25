package com.splicemachine.pipeline.constraint;

/**
 * Marker interface for Foreign-key violations
 * @author Scott Fines
 *         Date: 1/4/16
 */
public interface ForeignKeyViolation{
    ConstraintContext getContext();
}
