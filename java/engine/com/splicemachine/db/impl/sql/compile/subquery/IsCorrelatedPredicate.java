package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.impl.sql.compile.ColumnReference;

public class IsCorrelatedPredicate implements com.google.common.base.Predicate<ColumnReference> {
    @Override
    public boolean apply(ColumnReference columnReference) {
        return columnReference.getCorrelated();
    }
}