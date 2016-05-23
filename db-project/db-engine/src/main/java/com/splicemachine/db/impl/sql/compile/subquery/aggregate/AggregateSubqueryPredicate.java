package com.splicemachine.db.impl.sql.compile.subquery.aggregate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;
import java.util.List;

/**
 * This predicate determines if we attempt to flatten a given aggregate SubqueryNode or not.
 */
class AggregateSubqueryPredicate implements org.sparkproject.guava.base.Predicate<SubqueryNode> {

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
        /* Find column references in the aggregate expression */
        List<ColumnReference> aggregateColumnRefs = RSUtils.collectNodes(resultColumns.elementAt(0).getExpression(), ColumnReference.class);

        /* subquery where clause must meet several conditions */
        ValueNode whereClause = subquerySelectNode.getWhereClause();
        /* If there is no where clause on the subquery then ok */
        if (whereClause != null) {
            AggregateSubqueryWhereVisitor aggregateSubqueryWhereVisitor = new AggregateSubqueryWhereVisitor(subquerySelectNode.getNestingLevel(), aggregateColumnRefs);
            whereClause.accept(aggregateSubqueryWhereVisitor);
            return !aggregateSubqueryWhereVisitor.isFoundUnsupported();
        }
        return true;
    }


}
