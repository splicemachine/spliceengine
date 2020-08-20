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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelationLevelPredicate;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;
import java.util.List;
import static splice.com.google.common.collect.Iterables.any;
import static splice.com.google.common.collect.Iterables.filter;

/**
 * Encapsulates/implements current restrictions on the where-clause of subqueries in aggregate subquery flattening. We
 * consider the where-clause of a given subquery to allow flattening if:
 *
 * <pre>
 *
 * 1. The where clause is empty; or
 * 2. The root of the where clause is an AND or BRON node; and
 * 3. Correlated column references (if any) reference columns only one level up; and
 * 4. Correlated column references (if any) are part of an equality BRON; and
 * 5. Any correlated column references are not compared to subquery's the aggregated column.
 *
 * For example a subquery with this where-clause three can be flattened:
 *
 * WHERE a1=b1 AND a2=b2 AND b3>1 AND expression
 *
 *               AND
 *             /    \
 *           AND     BRON (a1=b1)
 *          /   \
 *        AND  BRON (a2=b2)
 *        /  \
 * BRON(b3>1) expression
 *
 * Where BRON = BinaryRelationalOperatorNode and expression is any uncorrelated subtree (including subqueries).
 *
 * </pre>
 * ------------------------------------------------------------- EXAMPLE 1:
 *
 * select A.* from A where a1 = (select sum(b2) from B where b1=a1)
 *
 * We can flatten this.  Becomes:
 *
 * select A.* from A join (select b1, sum(b2) s from B group by b1) foo on foo.b1 = a1 where a1 = foo.s
 *
 * ------------------------------------------------------------- EXAMPLE 2:
 *
 * select A.* from A where a1 = (select sum(b2) from B where b2=a1)
 *
 * We can NOT flatten this. When the subquery is moved to outer's from-list a1 cannot be joined with b2 which gets
 * aggregated/summed.
 *
 * ------------------------------------------------------------- EXAMPLE 3:
 *
 * select A.* from A where a1 = (select sum(b2) from B where b1 > a1)
 *
 * We can NOT flatten this. When the subquery is moved to outer's from-list the join on b1 > a1 could generate more rows
 * than exist in A.
 *
 * ------------------------------------------------------------- EXAMPLE 4:
 *
 * select A.* from A where a1 = (select sum(b2) from B where b1=a1 and b3 >= 40)
 *
 * We can flatten this. The expressions/predicates on B can be complex, or even subqueries.  As long as there are no
 * correlated column references everything gets moved to the outer's from-list.
 *
 *
 * Some of the restrictions here are fundamental and cannot be removed given our current mechanism for aggregate
 * subquery flattening. Others are limitations of the current implementation and could be removed or loosened in the
 * future.
 */
class AggregateSubqueryWhereVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(AggregateSubqueryWhereVisitor.class);

    /* The level of the subquery we are considering flattening in the enclosing predicate */
    private final int subqueryLevel;
    private final List<ColumnReference> aggColReferences;

    private boolean foundUnsupported;

    public AggregateSubqueryWhereVisitor(int subqueryLevel, List<ColumnReference> aggColReferences) {
        this.subqueryLevel = subqueryLevel;
        this.aggColReferences = aggColReferences;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            return node;
        }
        if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bro = (BinaryRelationalOperatorNode) node;
            ValueNode rightOperand = bro.getRightOperand();
            ValueNode leftOperand = bro.getLeftOperand();

            List<ColumnReference> leftReferences = RSUtils.collectNodes(leftOperand, ColumnReference.class);
            List<ColumnReference> rightReferences = RSUtils.collectNodes(rightOperand, ColumnReference.class);

            List<ColumnReference> leftReferencesCorrelated = Lists.newArrayList(filter(leftReferences, new ColumnUtils.IsCorrelatedPredicate()));
            List<ColumnReference> rightReferencesCorrelated = Lists.newArrayList(filter(rightReferences, new ColumnUtils.IsCorrelatedPredicate()));

            /* GOOD: Neither side had correlated predicates at any level. */
            if (leftReferencesCorrelated.isEmpty() && rightReferencesCorrelated.isEmpty()) {
                return node;
            }

            /* At this point we know the current BRON of the subquery where clause has correlated column references
             * on one side or the other. */

            /* BAD: Correlated predicates can only appear under an equality BRON, else we will get extra rows
             * when we convert the subquery to a join. */
            if (bro.getOperator() != RelationalOperator.EQUALS_RELOP) {
                foundUnsupported = true;
                return node;
            }

            /* BAD: We found a correlated column reference that references two or more levels up */
            CorrelationLevelPredicate correlationLevelPredicate = new CorrelationLevelPredicate(subqueryLevel);
            if (any(leftReferencesCorrelated, correlationLevelPredicate) || any(rightReferencesCorrelated, correlationLevelPredicate)) {
                foundUnsupported = true;
                return node;
            }

            /* BAD: When we have correlated predicates in the subquery they can only be CR directly under BRON for now.
             * This might look like a1*20 = b1*10, for example. */
            if (!(rightOperand instanceof ColumnReference && leftOperand instanceof ColumnReference)) {
                foundUnsupported = true;
                return node;
            }

            ColumnReference leftReference = (ColumnReference) leftOperand;
            ColumnReference rightReference = (ColumnReference) rightOperand;

            /* BAD: Correlated column reference cannot be compared to columns in the subqueries aggregate expression. */
            if (leftReference.getCorrelated() && aggColReferences.contains(rightReference)
                    ||
                    rightReference.getCorrelated() && aggColReferences.contains(leftReference)) {
                foundUnsupported = true;
                return node;
            }
            return node;
        }
        /* Top level node is not an AndNode or BinaryRelationalOperatorNode */
        else {
            /* Can be anything as long as it is not correlated. */
            foundUnsupported = ColumnUtils.isSubtreeCorrelated(node);
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        boolean isAndNode = node instanceof AndNode;
        return !isAndNode;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return foundUnsupported;
    }

    public boolean isFoundUnsupported() {
        return foundUnsupported;
    }

}
