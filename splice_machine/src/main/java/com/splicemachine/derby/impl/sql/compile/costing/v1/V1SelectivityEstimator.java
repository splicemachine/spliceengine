package com.splicemachine.derby.impl.sql.compile.costing.v1;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.FromTable;

public class V1SelectivityEstimator implements SelectivityEstimator {

    @Override
    public double defaultJoinSelectivity(long outerRowCount,
                                         long innerRowCount,
                                         SelectivityJoinType selectivityJoinType) {
        double selectivity = 0.0d;
        switch (selectivityJoinType) {
            case LEFTOUTER:
            case FULLOUTER:
                selectivity = 1.0d / innerRowCount;
                break;
            case INNER:
                selectivity = (1.0d - .1d) * (1.0d - .1d) / Math.min(innerRowCount, outerRowCount);
                break;
            case ANTIJOIN:
                selectivity = 1.0d - ((1.0d - .1d) * (1.0d - .1d) / Math.min(innerRowCount, outerRowCount));
                break;
        }
        return selectivity;
    }

    @Override
    public double innerJoinSelectivity(Optimizable rightTable,
                                       long leftRowCount,
                                       long rightRowCount,
                                       ColumnReference leftColumn,
                                       ColumnReference rightColumn,
                                       SelectivityJoinType selectivityJoinType) throws StandardException {
        double selectivity = 0.0d;
        selectivity = ((1.0d - leftColumn.nullSelectivity()) * (1.0d - rightColumn.nullSelectivity())) /
                Math.min(leftColumn.nonZeroCardinality(leftRowCount), rightColumn.nonZeroCardinality(rightRowCount));
        selectivity = selectivityJoinType.equals(SelectivityEstimator.SelectivityJoinType.INNER) ?
                selectivity : 1.0d - selectivity;
        if (rightTable instanceof FromTable && ((FromTable) rightTable).getExistsTable()) {
            selectivity = selectivity * leftColumn.nonZeroCardinality(leftRowCount)/leftRowCount;
            if ((rightTable instanceof FromBaseTable) && ((FromBaseTable) rightTable).isAntiJoin()) {
                selectivity = selectivity /(rightRowCount - (double)rightRowCount/rightColumn.nonZeroCardinality(rightRowCount) + 1);
            }
        }
        return selectivity;
    }

    @Override
    public double leftOuterJoinSelectivity(long leftRowCount, long rightRowCount,
                                           ColumnReference leftColumn, ColumnReference rightColumn) throws StandardException {
        return (1.0d - rightColumn.nullSelectivity()) / rightColumn.nonZeroCardinality(rightRowCount);
    }

    @Override
    public double fullOuterJoinSelectivity(long leftRowCount, long rightRowCount,
                                           ColumnReference leftColumn, ColumnReference rightColumn) throws StandardException {
        // TODO DB-7816, temporarily borrow the selectivity logic from left join, may need to revisit
        return leftOuterJoinSelectivity(leftRowCount, rightRowCount, leftColumn, rightColumn);
    }

    @Override
    public boolean checkJoinSelectivity(double selectivity) {
        return selectivity >= 0.0d;
    }
}
