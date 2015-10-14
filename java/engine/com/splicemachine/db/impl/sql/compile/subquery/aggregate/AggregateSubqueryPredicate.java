package com.splicemachine.db.impl.sql.compile.subquery.aggregate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.*;
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
        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();

        /* subquery cannot contain a union */
        if(subqueryResultSet.getFromList().containsNode(UnionNode.class)) {
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
        /* Make a modest attempt to find AggregateNode nodes so that can enforce restrictions on which columns their
         * ColumnReference operands can reference.  This code currently supports expressions like: sum(b1), 5*sum(b1),
         * sum(b1)*5, but otherwise gives up and returns false meaning we won't attempt flattening.  On second thought
         * I should have used RSUtils or similar to just traverse the ResultColumns and find all AggregateNodes here.
         * If we ever need to support flattening something like sum(b1)*sum(b2) we will have to do that. */
        ResultColumn resultColumn = resultColumns.elementAt(0);
        ValueNode expression = resultColumn.getExpression();
        AggregateNode aggregateNode;
        if (expression instanceof AggregateNode) {
            aggregateNode = (AggregateNode) expression;
        } else if (expression instanceof BinaryArithmeticOperatorNode ) {
            BinaryArithmeticOperatorNode bao = (BinaryArithmeticOperatorNode) expression;
            if ((bao.getLeftOperand() instanceof AggregateNode) && (bao.getRightOperand() instanceof ConstantNode)) {
                aggregateNode = (AggregateNode) bao.getLeftOperand();
            } else if ((bao.getRightOperand() instanceof AggregateNode) && (bao.getLeftOperand() instanceof ConstantNode)) {
                aggregateNode = (AggregateNode) bao.getRightOperand();
            } else {
                return false;
            }
        } else {
            /* Don't know how to find the ColumnReference inside of AggregateNode for use in checks below. */
            return false;
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
