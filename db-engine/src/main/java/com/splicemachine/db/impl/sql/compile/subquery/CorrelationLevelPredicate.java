package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.impl.sql.compile.ColumnReference;
import org.sparkproject.guava.base.Predicate;

/**
 * Returns true if the column reference is referring to a column MORE than one level up.
 */
public class CorrelationLevelPredicate implements Predicate<ColumnReference> {

    private int subqueryLevel;

    public CorrelationLevelPredicate(int subqueryLevel) {
        this.subqueryLevel = subqueryLevel;
    }

    @Override
    public boolean apply(ColumnReference columnReference) {
        return columnReference.getCorrelated() && columnReference.getSourceLevel() < subqueryLevel - 1;
    }
}