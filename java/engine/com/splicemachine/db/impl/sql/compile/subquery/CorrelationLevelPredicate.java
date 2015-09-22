package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.impl.sql.compile.ColumnReference;

/**
 * Returns true if the column reference is referring to a column MORE than one level up.
 */
public class CorrelationLevelPredicate implements com.google.common.base.Predicate<ColumnReference> {

    private int subqueryLevel;

    public CorrelationLevelPredicate(int subqueryLevel) {
        this.subqueryLevel = subqueryLevel;
    }

    @Override
    public boolean apply(ColumnReference columnReference) {
        return columnReference.getCorrelated() && columnReference.getSourceLevel() < subqueryLevel - 1;
    }
}