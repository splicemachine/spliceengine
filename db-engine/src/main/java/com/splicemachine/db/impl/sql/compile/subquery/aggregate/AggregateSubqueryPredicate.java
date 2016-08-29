/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile.subquery.aggregate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;
import java.util.List;

/**
 * This predicate determines if we attempt to flatten a given aggregate SubqueryNode or not.
 */
class AggregateSubqueryPredicate implements org.spark_project.guava.base.Predicate<SubqueryNode> {

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
