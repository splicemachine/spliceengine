package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 *
 * Interface for Representing a selectivity operation.
 *
 * Implementations of SelectivityHolder will perform the low level ops.
 *
 * Implements Comparable so we can easily sort Selectivities in ascending order for computation.
 *
 */
public interface SelectivityHolder extends Comparable<SelectivityHolder> {
        public double getSelectivity() throws StandardException;
        public QualifierPhase getPhase();
        public int getColNum();
        public boolean isRangeSelectivity();
}
