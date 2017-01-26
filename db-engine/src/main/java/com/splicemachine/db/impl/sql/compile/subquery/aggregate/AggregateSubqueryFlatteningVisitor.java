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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.subquery.aggregate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.*;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;
import java.util.ArrayList;
import java.util.List;


/**
 * Subquery flattening for where-subqueries with aggregates.  Splice added this class.  Previously derby explicitly did
 * not attempt to flatten where-subqueries with aggregates.
 *
 * How it works: We move the aggregate subquery into the enclosing query's FromList, add a GroupBy node and a table
 * alias, and update column references. From there we let derby further flatten the FromSubquery into a join.  The
 * result is an execution-time plan that has no subqueries.
 *
 * See nested class DoWeHandlePredicate for a list of current restrictions on the query types we support.
 *
 * In the context of this class "top" means the outer, or enclosing, select for the subquery(s) we are currently
 * attempting to flatten.
 *
 * See also: SubqueryNode.flattenToNormalJoin() SubqueryNode.flattenToExistsJoin() FromSubquery.flatten()
 */
public class AggregateSubqueryFlatteningVisitor extends AbstractSpliceVisitor implements Visitor {

    private final int originalNestingLevel;
    private int flattenedCount = 0;
    private SubqueryNodeFactory nf;

    public AggregateSubqueryFlatteningVisitor(int originalNestingLevel) {
        this.originalNestingLevel = originalNestingLevel;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return node instanceof SelectNode && !(((SelectNode) node).getWhereClause() instanceof BinaryOperatorNode);
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {

        /**
         * Stop if this is not a select node with a BinaryOperator where
         */
        if (!(node instanceof SelectNode) || !(((SelectNode) node).getWhereClause() instanceof BinaryOperatorNode)) {
            return node;
        }

        SelectNode topSelectNode = (SelectNode) node;

        /**
         * Stop if there are no subqueries.
         */
        if (topSelectNode.getWhereSubquerys() == null || topSelectNode.getWhereSubquerys().size() == 0) {
            return node;
        }

        /*
         * Stop if there are no subqueries we handle.
         */
        List<SubqueryNode> subqueryList = topSelectNode.getWhereSubquerys().getNodes();
        List<SubqueryNode> handledSubqueryList = Lists.newArrayList(Iterables.filter(subqueryList, new AggregateSubqueryPredicate()));
        if (handledSubqueryList.isEmpty()) {
            return node;
        }

        /*
         * Flatten where applicable.
         */
        CompilerContext cpt = topSelectNode.getCompilerContext();
        cpt.setNumTables(cpt.getNumTables() + handledSubqueryList.size());
        nf = new SubqueryNodeFactory(topSelectNode.getContextManager(), topSelectNode.getNodeFactory());

        for (SubqueryNode subqueryNode : handledSubqueryList) {
            flatten(topSelectNode, subqueryNode);
        }

        /*
         * Finally remove the flattened subquery nodes from the top select node.
         */
        for (SubqueryNode subqueryNode : handledSubqueryList) {
            topSelectNode.getWhereSubquerys().removeElement(subqueryNode);
        }

        return node;
    }

    /**
     * Perform the actual flattening (we start to mutate the tree at this point).
     */
    private void flatten(SelectNode topSelectNode,
                         SubqueryNode subqueryNode) throws StandardException {

        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;

        /**
         * The following lines collect correlated predicates from the subquery where clause while removing them.
         */
        ValueNode subqueryWhereClause = subquerySelectNode.getWhereClause();
        List<BinaryRelationalOperatorNode> correlatedSubqueryPreds = new ArrayList<>();
        subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(
                subqueryWhereClause,
                correlatedSubqueryPreds,
                new CorrelatedEqualityBronPredicate(topSelectNode.getNestingLevel()));
        subquerySelectNode.setWhereClause(subqueryWhereClause);
        subquerySelectNode.setOriginalWhereClause(subqueryWhereClause);

        /*
         * For each correlated predicate generate a GroupByColumn
         */
        GroupByUtil.addGroupByNodes(subquerySelectNode, correlatedSubqueryPreds);

        ResultColumnList newRcl = subquerySelectNode.getResultColumns().copyListAndObjects();
        newRcl.genVirtualColumnNodes(subquerySelectNode, subquerySelectNode.getResultColumns());

        /*
         * Insert the new FromSubquery into to origSelectNode's From list.
         */
        FromSubquery fromSubquery = nf.buildFromSubqueryNode(topSelectNode, subqueryResultSet, newRcl, getSubqueryAlias());
        topSelectNode.getFromList().addFromTable(fromSubquery);

        /*
         * Add correlated predicates from subquery to outer query where clause.
         */
        ValueNode newTopWhereClause = SubqueryReplacement.replaceSubqueryWithColRef(topSelectNode.getWhereClause(), fromSubquery, topSelectNode.getNestingLevel());
        for (int i = 0; i < correlatedSubqueryPreds.size(); i++) {
            BinaryRelationalOperatorNode pred = correlatedSubqueryPreds.get(i);

            int fromSubqueryColIndex = i + 1;
            ColumnReference colRef = FromSubqueryColRefFactory.build(topSelectNode.getNestingLevel(), fromSubquery,
                    fromSubqueryColIndex, topSelectNode.getNodeFactory(), topSelectNode.getContextManager());

            FromSubqueryColRefFactory.replace(pred, colRef, topSelectNode.getNestingLevel() + 1);

            /*
             * Finally add the predicate to the outer query.
             */
            newTopWhereClause = FlatteningUtils.addPredToTree(newTopWhereClause, pred);
        }

        topSelectNode.setOriginalWhereClause(newTopWhereClause);
        topSelectNode.setWhereClause(newTopWhereClause);
    }

    private String getSubqueryAlias() {
        return String.format("AggFlatSub-%s-%s", originalNestingLevel, ++flattenedCount);
    }

}