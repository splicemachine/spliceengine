package com.splicemachine.derby.impl.sql.compile.costing.v2;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.FromTable;

public class V2SelectivityEstimator implements SelectivityEstimator {

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
        // For the join, cardinality of LHS and RHS are not necessarily the raw numbers from column
        // statistics. We should apply them on LHS and RHS output row counts proportionally.
        double leftColumnIndexSelectivity = leftColumn.nonZeroCardinality(leftRowCount) / leftColumn.rowCountEstimate();
        double rightColumnIndexSelectivity = rightColumn.nonZeroCardinality(rightRowCount) / rightColumn.rowCountEstimate();
        double leftNonZeroCardinality = Math.max(1.0, leftColumnIndexSelectivity * leftRowCount);
        double rightNonZeroCardinality = Math.max(1.0, rightColumnIndexSelectivity * rightRowCount);

        selectivity = ((1.0d - leftColumn.nullSelectivity()) * (1.0d - rightColumn.nullSelectivity())) /
                Math.max(leftNonZeroCardinality, rightNonZeroCardinality);
        selectivity = selectivityJoinType.equals(SelectivityEstimator.SelectivityJoinType.INNER) ?
                selectivity : 1.0d - selectivity;
        if (rightTable instanceof FromTable && ((FromTable) rightTable).getExistsTable()) {
            selectivity = selectivity * leftNonZeroCardinality / leftRowCount;
            if ((rightTable instanceof FromBaseTable) && ((FromBaseTable) rightTable).isAntiJoin()) {
                selectivity = selectivity /(rightRowCount - (double)rightRowCount / rightNonZeroCardinality + 1);
            }
        }
        return selectivity;
    }

    @Override
    public double leftOuterJoinSelectivity(long leftRowCount, long rightRowCount,
                                           ColumnReference leftColumn, ColumnReference rightColumn) throws StandardException {
        double rightColumnIndexSelectivity = rightColumn.nonZeroCardinality(rightRowCount) / rightColumn.rowCountEstimate();
        double rightNonZeroCardinality = Math.max(1.0, rightColumnIndexSelectivity * rightRowCount);
        return (1.0d - rightColumn.nullSelectivity()) / rightNonZeroCardinality;
    }

    @Override
    public double fullOuterJoinSelectivity(long leftRowCount, long rightRowCount,
                                           ColumnReference leftColumn, ColumnReference rightColumn) throws StandardException {
        // TODO DB-7816, temporarily borrow the selectivity logic from left join, may need to revisit
        return leftOuterJoinSelectivity(leftRowCount, rightRowCount, leftColumn, rightColumn);
    }

    @Override
    public boolean checkJoinSelectivity(double selectivity) {
        return selectivity >= 0.0d && selectivity <= 1.0d;
    }
}
