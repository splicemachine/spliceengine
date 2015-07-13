package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.log4j.Logger;

/**
 * This predicate determines if we attempt to flatten a given aggregate SubqueryNode or not.
 */
class AggregateSubqueryPredicate implements com.google.common.base.Predicate<SubqueryNode> {

    private static Logger LOG = Logger.getLogger(AggregateSubqueryPredicate.class);

    @Override
    public boolean apply(SubqueryNode subqueryNode) {
        try {
            return doWeHandle(subqueryNode);
        } catch (StandardException e) {
            LOG.error("unexpected exception while considering aggregate subquery flattening", e);
            return false;
        }
    }

    private boolean doWeHandle(SubqueryNode subqueryNode) throws StandardException {
        /* subquery cannot be a union */
        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();
        if (subqueryResultSet.getFromList().elementAt(0) instanceof UnionNode) {
            return false;
        }
        /* subquery must be a select node */
        if (!(subqueryResultSet instanceof SelectNode)) {
            return false;
        }
        /* subquery must have an aggregate in select list */
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;
        if (!subquerySelectNode.hasAggregatesInSelectList()) {
            return false;
        }
        /* subquery must have exactly one result column */
        ResultColumnList resultColumns = subqueryResultSet.getResultColumns();
        if (resultColumns.size() != 1) {
            return false;
        }
        /* subquery aggregation operand must be a column reference or column reference arithmetic.  That is sum(b2) or
         * 5*sum(b2) not sum(5*b2) or sum(b2)*avg(b3).  This is arbitrary, the real restriction is that none of the
         * aggregate columns can be referenced by the subquery's correlated predicates because these get moved to the
         * outer query which cannot reference columns that get aggregated. We could possibly loosen this restriction
         * in the future.*/
        ResultColumn resultColumn = resultColumns.elementAt(0);
        ValueNode expression = resultColumn.getExpression();
        AggregateNode aggregateNode;
        if (expression instanceof AggregateNode) {
            aggregateNode = (AggregateNode) expression;
        } else {
            BinaryArithmeticOperatorNode bao = (BinaryArithmeticOperatorNode) expression;
            if ((bao.getLeftOperand() instanceof AggregateNode) && (bao.getRightOperand() instanceof ConstantNode)) {
                aggregateNode = (AggregateNode) bao.getLeftOperand();
            } else if ((bao.getRightOperand() instanceof AggregateNode) && (bao.getLeftOperand() instanceof ConstantNode)) {
                aggregateNode = (AggregateNode) bao.getRightOperand();
            } else {
                return false;
            }
        }
        ValueNode aggregateNodeOperand = aggregateNode.getOperand();
        if (!(aggregateNodeOperand instanceof ColumnReference)) {
            return false;
        }

        /* subquery where clause must meet several conditions */
        ValueNode whereClause = subquerySelectNode.getWhereClause();
        /* If there is no where clause on the subquery then ok */
        if (whereClause != null) {
            ColumnReference aggregationColumnReference = (ColumnReference) aggregateNode.getOperand();
            AggregateSubqueryWhereVisitor aggregateSubqueryWhereVisitor = new AggregateSubqueryWhereVisitor(subquerySelectNode.getNestingLevel(), aggregationColumnReference);
            whereClause.accept(aggregateSubqueryWhereVisitor);
            return !aggregateSubqueryWhereVisitor.isFoundUnsupported();
        }
        return true;
    }


}
