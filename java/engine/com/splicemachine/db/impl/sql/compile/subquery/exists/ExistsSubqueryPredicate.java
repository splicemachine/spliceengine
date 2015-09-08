package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;

/**
 * This predicate determines if we attempt to flatten a given aggregate SubqueryNode or not.
 */
class ExistsSubqueryPredicate implements com.google.common.base.Predicate<SubqueryNode> {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryPredicate.class);

    @Override
    public boolean apply(SubqueryNode subqueryNode) {
        try {
            return doWeHandle(subqueryNode);
        } catch (StandardException e) {
            LOG.error("unexpected exception while considering exists subquery flattening", e);
            return false;
        }
    }

    private boolean doWeHandle(SubqueryNode subqueryNode) throws StandardException {

        if (subqueryNode.getSubqueryType() != SubqueryNode.EXISTS_SUBQUERY) {
            return false;
        }

        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();

        /* subquery cannot contain a union */
        FromList fromList = subqueryResultSet.getFromList();
        for (Object fromNodeList : fromList.getNodes()) {
            if (fromNodeList instanceof UnionNode) {
                return false;
            }
        }

        if(fromList.size() == 1 && fromList.elementAt(0) instanceof FromBaseTable) {
            /* Derby can already flatten this, we don't need to attempt. */
            return false;
        }

        /* subquery must be a select node */
        if (!(subqueryResultSet instanceof SelectNode)) {
            return false;
        }

        /* subquery where clause must meet several conditions */
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;

        /* subquery where clause must meet several conditions */
        ValueNode whereClause = subquerySelectNode.getWhereClause();

        /* If there is no where clause on the exist subquery then it is not correlated and we can't flatten */
        if (whereClause == null) {
            return false;
        }
        /* If there is no where clause on the subquery then ok */
        ExistsSubqueryWhereVisitor subqueryWhereVisitor = new ExistsSubqueryWhereVisitor(subquerySelectNode.getNestingLevel());
        whereClause.accept(subqueryWhereVisitor);
        return subqueryWhereVisitor.isCorrelatedEquality() && !subqueryWhereVisitor.isFoundUnsupported();
    }


}
