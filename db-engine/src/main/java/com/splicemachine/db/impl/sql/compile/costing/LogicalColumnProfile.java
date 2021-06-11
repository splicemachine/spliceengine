package com.splicemachine.db.impl.sql.compile.costing;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.*;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;

import java.text.DecimalFormat;
import java.util.List;

public class LogicalColumnProfile implements Cloneable {
    enum JoinType {
        INNER,
        SEMI,
        SELF
    }

    private final ResultColumn resultColumn;

    private double distinctCount;
    private DataValueDescriptor minValue;
    private DataValueDescriptor maxValue;
    private boolean distinctCountUpdated;

    private static final DecimalFormat df = new DecimalFormat("#.#");

    public LogicalColumnProfile(ResultColumn rc, double distinctCount, DataValueDescriptor minValue, DataValueDescriptor maxValue) {
        this.resultColumn = rc;
        this.distinctCount = distinctCount;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.distinctCountUpdated = false;
    }

    public double getDistinctCount() {
        return distinctCount;
    }

    public DataValueDescriptor getMinValue() {
        return minValue;
    }

    public DataValueDescriptor getMaxValue() {
        return maxValue;
    }

    /**
     * Compute a new logical profile for this column if the column is referenced in a
     * single-table non-correlated predicate.
     *
     * @param holder A filter predicate referencing this column in its selectivity holder.
     */
    public void select(SelectivityHolder holder) throws StandardException {
        if (holder instanceof RangeSelectivity) {
            RangeSelectivity rs = (RangeSelectivity) holder;
            if (rs.includeStart && rs.includeStop) {
                if (rs.start != null && rs.start.equals(rs.stop)
                        || (rs.start == null || rs.start.isNull()) && (rs.stop == null || rs.stop.isNull())) {
                    distinctCount = 1;
                    distinctCountUpdated = true;
                    minValue = rs.start;
                    maxValue = rs.start;
                } else {
                    if (rs.start != null && !rs.start.isNull()) {
                        minValue = max(minValue, rs.start);
                    }
                    if (rs.stop != null && !rs.stop.isNull()) {
                        maxValue = min(maxValue, rs.stop);
                    }
                    // TODO: update d if numeric
                }
            } else {
                minValue = max(minValue, rs.start);
                maxValue = min(maxValue, rs.stop);
            }
        } else if (holder instanceof NullSelectivity) {
            distinctCount = 1;
            distinctCountUpdated = true;
            minValue = resultColumn.getTypeServices().getNull();
            maxValue = minValue;
        } else if (holder instanceof NotNullSelectivity || holder instanceof NotEqualsSelectivity) {
            distinctCount -= 1;
            distinctCountUpdated = true;
        } else if (holder instanceof InListSelectivity) {
            //
        } else if (holder instanceof DefaultPredicateSelectivity) {
            // currently not supported:
            // A = B
            // A <= B
            // E1 or E2
        }
    }

    /**
     * Compute a new logical profile for this column if the column is referenced in a
     * single-table correlated predicate.
     *
     * @param other Logical profile of the other column referenced in the single-
     *              table correlated predicate.
     */
    public void select(int opType, LogicalColumnProfile other) throws StandardException {
        switch (opType) {
            case BinaryOperatorNode.EQ:
                distinctCount = Math.min(distinctCount, other.getDistinctCount());
                distinctCountUpdated = true;
                minValue = max(minValue, other.getMinValue());
                maxValue = min(maxValue, other.getMaxValue());
                break;
            case BinaryOperatorNode.LE:
            case BinaryOperatorNode.LT:
                maxValue = min(maxValue, other.getMaxValue());
                break;
            case BinaryOperatorNode.GE:
            case BinaryOperatorNode.GT:
                minValue = max(minValue, other.getMinValue());
                break;
        }
    }

    /**
     * Compute a new logical profile for this column if the column is referenced in an
     * equi-join predicate.
     *
     * @param joinType One of LogicalColumnProfile.JoinType.
     * @param other    The logical profile of the other column referenced in the same
     *                 join predicate.
     * @return A new logical profile for this column in the result relation.
     */
    public LogicalColumnProfile equiJoin(JoinType joinType, LogicalColumnProfile other) throws StandardException {
        switch (joinType) {
            case SELF:
                return this.clone();
            case INNER:
            case SEMI:
            default:
                return new LogicalColumnProfile(resultColumn, Math.min(distinctCount, other.getDistinctCount()),
                                                max(minValue, other.getMaxValue()),
                                                min(maxValue, other.getMaxValue()));
        }
    }

    public LogicalColumnProfile union(LogicalColumnProfile other) {
        return null;
    }

    public LogicalColumnProfile intersect(LogicalColumnProfile other) {
        return null;
    }

    /**
     * Compute the distinct number of rows of the result relation after a group
     * operation. For group, this is also the result cardinality.
     *
     * @param inputRowCount  Row count before grouping.
     * @param groupingColumnDistinctCounts  A list of distinct counts of all
     *                                      grouping columns.
     * @return The distinct number of rows after grouping.
     */
    public static double groupDistinctCount(double inputRowCount, List<Double> groupingColumnDistinctCounts) {
        if (groupingColumnDistinctCounts.size() == 1) {
            return groupingColumnDistinctCounts.get(0);
        }
        double n = 1;
        for (double gcdc : groupingColumnDistinctCounts) {
            n *= gcdc;
        }
        return Math.max((n * inputRowCount) / (n + inputRowCount - 1), 1.0);
    }

    /**
     * Compute a new logical profile of this column if the column is not referenced in
     * predicates of current operator.
     *
     * @param inputRowCount  Row count of the input relation.
     * @param outputRowCount Row count of the output relation.
     * @return A new logical profile of this column for the output relation.
     */
    public void updateDistinctCount(double inputRowCount, double outputRowCount) {
        if (!distinctCountUpdated) {
            distinctCount = getDistinctCountUsingSelectivity(inputRowCount, outputRowCount);
        }
        // reset state
        distinctCountUpdated = false;
    }

    /**
     * Limit the distinct count of this logical profile to a known upper bound.
     *
     * @param upperBound The known upper bound of distinct count.
     */
    public void limitDistinctCount(double upperBound) {
        distinctCount = Math.min(distinctCount, upperBound);
    }

    @Override
    public LogicalColumnProfile clone() {
        return new LogicalColumnProfile(resultColumn, distinctCount, minValue, maxValue);
    }

    public String getString() throws StandardException {
        return "(" + resultColumn.getColumnPosition() + "," + df.format(distinctCount) + ","
                + minValue.getString() + "," + maxValue.getString() + ")";
    }

    private DataValueDescriptor min(DataValueDescriptor l, DataValueDescriptor r) throws StandardException {
        int compVal = l.compare(r);
        return compVal <= 0 ? l : r;
    }

    private DataValueDescriptor max(DataValueDescriptor l, DataValueDescriptor r) throws StandardException {
        int compVal = l.compare(r);
        return compVal < 0 ? r : l;
    }

    private double getDistinctCountUsingSelectivity(double inputRowCount, double outputRowCount) {
        if (inputRowCount == 0) {
            inputRowCount = 1.0;
        }
        if (outputRowCount == 0) {
            outputRowCount = 1.0;
        }
        double sel = Math.min(outputRowCount / inputRowCount, 1.0);
        return distinctCount * (1 - Math.pow(1 - sel, inputRowCount / distinctCount));
    }
}
