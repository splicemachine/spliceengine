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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.List;

/**
 * If a RCL (SELECT list) contains an aggregate, then we must verify
 * that the RCL (SELECT list) is valid.
 * For ungrouped queries,
 * the RCL must be composed entirely of valid aggregate expressions -
 * in this case, no column references outside of an aggregate.
 * For grouped aggregates,
 * the RCL must be composed of grouping columns or valid aggregate
 * expressions - in this case, the only column references allowed outside of
 * an aggregate are grouping columns and SSQs correlating on the grouping columns.
 */
public class VerifyAggregateExpressionsVisitor implements Visitor {
    private GroupByList groupByList;

    public VerifyAggregateExpressionsVisitor(GroupByList groupByList) {
        this.groupByList = groupByList;
    }


    ////////////////////////////////////////////////
    //
    // VISITOR INTERFACE
    //
    ////////////////////////////////////////////////

    /**
     * Verify that this expression is ok
     * for an aggregate query.
     *
     * @param node the node to process
     * @throws StandardException on ColumnReference not in group by list,
     * ValueNode or JavaValueNode that isn't under an aggregate.
     */
    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference) node;

            if (groupByList == null) {
                throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_NON_GROUPED_SELECT_LIST, cr.getSQLColumnName());
            }

            if (groupByList.findGroupingColumn(cr) == null) {
                throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_GROUPED_SELECT_LIST, cr.getSQLColumnName());
            }
        }

        /*
         ** Subqueries are only valid if they do not have
         ** correlations and are expression subqueries.  RESOLVE:
         ** this permits VARIANT expressions in the subquery --
         ** should this be allowed?  may be confusing to
         ** users to complain about:
         **
         ** select max(x), (select sum(y).toString() from y) from x
         */
        else if (node instanceof SubqueryNode) {
            SubqueryNode subquery = (SubqueryNode) node;

            if ((subquery.getSubqueryType() != SubqueryNode.EXPRESSION_SUBQUERY) ||
                    subquery.hasCorrelatedCRs()) {
                if (groupByList != null && subquery.getSubqueryType() == SubqueryNode.EXPRESSION_SUBQUERY) {
                    /*
                     ** Check if the SSQ's correlated columns are referencing the grouping columns, if so, allow it.
                     */
                    List<ValueNode> correlationCRs = subquery.getCorrelationCRs();
                    for (ValueNode correlationCR : correlationCRs) {
                        if (correlationCR instanceof ColumnReference) {
                            if(groupByList.findGroupingColumn(correlationCR) == null) {
                                throw StandardException.newException(SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
                            }
                        } else {
                            throw StandardException.newException(SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
                        }
                    }
                } else {
                    throw StandardException.newException(SQLState.LANG_INVALID_NON_GROUPED_SELECT_LIST);
                }
            }
        } else if (node instanceof GroupingFunctionNode) {
            if (groupByList == null || !groupByList.isRollup()) {
                throw StandardException.newException(com.splicemachine.db.shared.common.reference.SQLState.LANG_FUNCTION_NOT_ALLOWED,
                        "GROUPING",
                        "Query without OLAP operations like rollup, cube, grouping sets");
            } else if (!((GroupingFunctionNode) node).isSingleColumnExpression()) {
                throw StandardException.newException(com.splicemachine.db.shared.common.reference.SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                        node.toString(),
                        "GROUPING");
            }
        }
        return node;
    }

    /**
     * Don't visit children under an aggregate, subquery or any node which
     * is equivalent to any of the group by expressions.
     *
     * @param node the node to process.
     */
    public boolean skipChildren(Visitable node) throws StandardException {
        // skip aggregate node but not window function node
        return ((node instanceof AggregateNode && !(node instanceof WindowFunctionNode)) ||
                (node instanceof SubqueryNode) ||
                (node instanceof ValueNode &&
                        groupByList != null
                        && groupByList.findGroupingColumn((ValueNode) node) != null));
    }

    public boolean stopTraversal() {
        return false;
    }

    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }
}	
