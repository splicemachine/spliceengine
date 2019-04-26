/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.FlatteningUtils;
import org.apache.log4j.Logger;
import java.util.List;

/**
 * This predicate determines if we attempt to flatten a given exists SubqueryNode or not.
 */
class ExistsSubqueryPredicate implements org.spark_project.guava.base.Predicate<SubqueryNode> {

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

        boolean existsSubquery = subqueryNode.isEXISTS();
        boolean notExistsSubquery = subqueryNode.isNOT_EXISTS();
        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();

        /* Must be an exists or not-exists subquery */
        if (!(existsSubquery || notExistsSubquery)) {
            return false;
        }

        /* subquery result set must be a select node */
        if (!(subqueryResultSet instanceof SelectNode)) {
            return false;
        }
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;

        /* Don't currently do NOT-EXISTS flattening when the outer select's fromList has multiple elements. If there
         * are multiple tables under a single join node in the FromList ok, but multiple elements in the FromList
         * is not currently supported. This just because I haven't yet figured out how to put multiple FromList elements
         * under the left side of the left join node we add for not-exists flattening.
         *
         * OK           : select * from A join B where not exists....
         * NOT FLATTENED: select * from A,B where not exists...
         *
         * */
        if (notExistsSubquery && outerSelectNode.getFromList().size() > 1) {
            return false;
        }

        /* Must be directly under an And in predicates */
        if (!subqueryNode.getUnderTopAndNode()) {
            return false;
        }

        /* subquery cannot have a limit or offset */
        if ((subqueryNode.getOffset() != null || subqueryNode.getFetchFirst() != null)) {
            return false;
        }

        /* If uncorrelated then we are finished */
        if (!ColumnUtils.isSubtreeCorrelated(subqueryNode)) {
            return true;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        // The subquery is correlated
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // For correlated exists/not exists subquery, if it is just a non-aggregate subquery with one table,
        // inner join(for exists) and anti-join(for not exists) with the base table directly(through the flattening logic
        // in SubqueryNode) are heuristically more efficient than rewriting the exists subquery to a derived table(in this module).
        // So for such cases, return false here and let the flattening logic in SubqueryNode take care of them.
        FromList fromList = subqueryResultSet.getFromList();
        if (subqueryNode.singleFromBaseTable(fromList) != null) {
            boolean hasAggregation = subquerySelectNode.hasAggregatesInSelectList() || subquerySelectNode.hasHavingClause()
                    || (subquerySelectNode.getGroupByList()!= null && subquerySelectNode.getGroupByList().isRollup());
            if (!hasAggregation &&
                    !subquerySelectNode.getOriginalWhereClauseHadSubqueries())
                return false;
        }

        /* correlated subquery cannot contain a union */
        if (subqueryResultSet.getFromList().containsNode(UnionNode.class)) {
            return isUnionSubqueryOk(subqueryNode, subquerySelectNode.getNestingLevel(), notExistsSubquery);
        }

        /* subquery where clause must meet several conditions */
        ValueNode whereClause = subquerySelectNode.getWhereClause();
        return whereClause == null || isWhereClauseOk(whereClause, notExistsSubquery, subquerySelectNode.getNestingLevel());
    }

    private boolean isWhereClauseOk(ValueNode whereClause, boolean notExistsSubquery, int nestingLevel) throws StandardException {
        ExistsSubqueryWhereVisitor subqueryWhereVisitor = new ExistsSubqueryWhereVisitor(nestingLevel, notExistsSubquery);
        whereClause.accept(subqueryWhereVisitor);
        return !subqueryWhereVisitor.isFoundUnsupported();
    }


    /**
     * The tree for a subquery with three unions looks like this:
     *
     * <pre>
     *           Subquery
     *              |
     *           Select
     *              |
     *           FromList
     *              |
     *            Union
     *           /    \
     *       Union   Select
     *      /    \
     *   Select  Select
     * </pre>
     *
     * Notice that #Select == #Union + 1
     *
     * The SQL looks like this:
     *
     * <pre>
     * from A
     * where exists ( select 1 from C where c1=a1
     *                  union
     *                select 1 from D where d1=a1
     *                  union
     *                select 1 from E where e1=a1
     *              )
     *
     * And our restrictions are:
     *
     * -- Each subquery can only have a single type D predicate.
     * -- There can be no type C predicates.
     * -- All type D predicates must reference the same column from the outer tables.
     * -- Correlated predicates can only reference one subquery table column per subquery (implicit in the above
     * restrictions).  That is we don't support subqueries with predicates like (a1=e2 and a1=e3).
     *
     * </pre>
     *
     * Why the additional restrictions for union subqueries?  Consider the join:
     *
     * <pre>
     *     from A
     *     join ( select c1 r from C union select d1 r from D union select e1 r from E ) foo
     *     where a1 = foo.r
     * </pre>
     *
     * If the subquery for table C contained predicate c1=a2 then how could this be added to the outer query?  a2=foo.r
     * would give the wrong result since a2 should ONLY be compared to values coming from table C.  So this case cannot
     * be handled without significant changes to how we join (we would have to select one column for each table or some
     * scheme like that).
     */
    private boolean isUnionSubqueryOk(SubqueryNode subqueryNode, int subqueryNestingLevel, boolean notExistsSubquery) throws StandardException {
        List<UnionNode> unionNodes = FlatteningUtils.findSameLevelUnionNodes(subqueryNode);
        List<SelectNode> selectNodes = FlatteningUtils.findSameLevelSelectNodes(subqueryNode);

        assert unionNodes.size() >= 1 && unionNodes.size() + 1 == selectNodes.size() : "Sanity check, one more select node than union node";

        ColumnReference outerColumnReference = null;
        for (SelectNode selectNode : selectNodes) {
            ExistsSubqueryWhereVisitor subqueryWhereVisitor = new ExistsSubqueryWhereVisitor(subqueryNestingLevel, notExistsSubquery);
            selectNode.getWhereClause().accept(subqueryWhereVisitor);

            /* No union subquery can have OR nodes, etc */
            if (subqueryWhereVisitor.isFoundUnsupported()) {
                return false;
            }
            /* None can have type C predicates */
            if (subqueryWhereVisitor.getTypeCCount() > 0) {
                return false;
            }
            /* None can have more than one type D predicate */
            List<ColumnReference> foundTypeDColRefs = subqueryWhereVisitor.getTypeDCorrelatedColumnReference();
            if (foundTypeDColRefs.size() != 1) {
                return false;
            }
            /* All type D predicates must reference the same outer column */
            ColumnReference foundTypeDColRef = foundTypeDColRefs.get(0);
            if (outerColumnReference != null && !outerColumnReference.isEquivalent(foundTypeDColRef)) {
                return false;
            }
            outerColumnReference = foundTypeDColRef;
        }

        return true;
    }

}
