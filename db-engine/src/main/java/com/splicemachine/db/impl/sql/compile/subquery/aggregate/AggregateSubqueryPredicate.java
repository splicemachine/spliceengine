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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.subquery.aggregate;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import splice.com.google.common.base.Function;
import splice.com.google.common.base.Predicates;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.log4j.Logger;
import java.util.List;

/**
 * This predicate determines if we attempt to flatten a given aggregate SubqueryNode or not.
 */
class AggregateSubqueryPredicate implements splice.com.google.common.base.Predicate<SubqueryNode> {

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

    public static final Function<Visitable, Object> valueProducingCheckerFn = visitable -> {
        if(visitable == null) {
            return null;
        }
        if(visitable instanceof AggregateNode) {
            AggregateNode aggregateNode = (AggregateNode)visitable;
            switch(aggregateNode.getType()) {
                case COUNT_STAR_FUNCTION: // fallthrough
                case COUNT_FUNCTION:
                    return visitable;
            }
        }
        return null;
    };

    private boolean isValueProducingExpr(ValueNode expr) throws StandardException {
        CollectingVisitor<QueryTreeNode> valueProducingFnVisitor = new CollectingVisitor<>(
                Predicates.or(
                        Predicates.instanceOf(CoalesceFunctionNode.class),
                        Predicates.compose(Predicates.instanceOf(Visitable.class), valueProducingCheckerFn)
                )
        );
        expr.accept(valueProducingFnVisitor);
        return !valueProducingFnVisitor.getCollected().isEmpty();
    }

    private boolean doWeHandle(SubqueryNode subqueryNode) throws StandardException {
        if (subqueryNode.isHintNotFlatten())
            return false;

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

        // if this is a scalar aggregate subquery, do not flatten it, as it is more performant to compute the subquery once and
        // cache and reuse it
        if (subqueryNode.isNonCorrelatedSubquery() &&
                (subquerySelectNode.getGroupByList() == null || subquerySelectNode.getGroupByList().isEmpty()))
            return false;

        /*
        * the aggregate must not be value-producing, flattening such aggregates could lead to wrong results since the resulting inner join
        * could miss some rows.
        * For example:
        *    select a,b from table1 where 0 = (select count(table2.a) from table2 where table2.a = table1.a)
        *     if table2 is empty, then count() will return 0 (value-producing), causing the where condition to be satisfied.
        *     however, if we flatten, then we end up with something like this:
        *    select table1.a,b from table1, (select table2.a, count(table2.a) from table2 group by table2.a having count(table2.a) = 0) X where table1.a = X.a
        *     which will return empty result since the RHS of the join is empty, and the join type is inner.
        */
        if(isValueProducingExpr(resultColumns.elementAt(0))) {
            return false;
        }

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
