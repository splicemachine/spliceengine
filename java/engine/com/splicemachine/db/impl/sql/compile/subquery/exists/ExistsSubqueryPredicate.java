package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;

/**
 * This predicate determines if we attempt to flatten a given exists SubqueryNode or not.
 */
class ExistsSubqueryPredicate implements com.google.common.base.Predicate<SubqueryNode> {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryPredicate.class);

    private SelectNode outerSelectNode;

    public ExistsSubqueryPredicate(SelectNode outerSelectNode) {
        this.outerSelectNode = outerSelectNode;
    }

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

        /* Must be an exists subquery */
        if (!(subqueryNode.isEXISTS() || subqueryNode.isNOT_EXISTS())) {
            return false;
        }

        /* Don't currently do NOT-EXISTS flattening when the outer select's fromList has multiple elements. If there
         * are multiple tables under a single join node in the FromList ok, but multiple elements in the FromList
         * is not currently supported. This just because I haven't yet figured out how to put multiple FromList elements
         * under the left side of the left join node we add for not-exists flattening.
         *
         * OK           : select * from A join B where not exists....
         * NOT FLATTENED: select * from A,B where not exists...
         *
         * */
        if (subqueryNode.isNOT_EXISTS() && outerSelectNode.getFromList().size() > 1) {
            return false;
        }

        /* Must be directly under an And in predicates */
        if (!subqueryNode.getUnderTopAndNode()) {
            return false;
        }

        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();

        /* subquery cannot contain a union */
        if (subqueryResultSet.getFromList().containsNode(UnionNode.class)) {
            return false;
        }

        /* subquery must be a select node */
        if (!(subqueryResultSet instanceof SelectNode)) {
            return false;
        }

        /* subquery where clause must meet several conditions */
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;
        ValueNode whereClause = subquerySelectNode.getWhereClause();
        /* If there is no where clause on the subquery then ok */
        if (whereClause == null) {
            return true;
        }
        ExistsSubqueryWhereVisitor subqueryWhereVisitor = new ExistsSubqueryWhereVisitor(subquerySelectNode.getNestingLevel(), subqueryNode.isNOT_EXISTS());
        whereClause.accept(subqueryWhereVisitor);
        return !subqueryWhereVisitor.isFoundUnsupported();
    }


}
