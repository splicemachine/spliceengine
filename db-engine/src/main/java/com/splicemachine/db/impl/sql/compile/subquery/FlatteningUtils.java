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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.sql.compile.*;
import java.util.List;

/**
 * Shared code used by our subquery flattening implementations that didn't fit elsewhere.
 */
public class FlatteningUtils {

    /**
     * Add predicates to the outer table's where clause.
     *
     * @param node is initially the where clause of the outer query.
     * @param pred is a predicate we are moving from subquery to the outer query.  It should already have column
     *             references updated to reference the new FromSubquery table.
     */
    public static ValueNode addPredToTree(ValueNode node,
                                          ValueNode pred) throws StandardException {
        if (node instanceof AndNode) {
            AndNode root = (AndNode) node;
            if (root.getLeftOperand() instanceof BinaryRelationalOperatorNode || root.getLeftOperand() instanceof BinaryListOperatorNode) {
                return newAndNode(node, pred, root);
            } else {
                root.setLeftOperand(addPredToTree(root.getLeftOperand(), pred));
                return root;
            }

        } else {
            return newAndNode(node, pred, node);
        }
    }

    private static ValueNode newAndNode(ValueNode node, ValueNode pred, ValueNode root) throws StandardException {
        ValueNode andNode = (AndNode) node.getNodeFactory().getNode(C_NodeTypes.AND_NODE,
                root,
                pred,
                node.getContextManager());
        andNode.setType(pred.getTypeServices());
        return andNode;
    }

    /**
     * Collects (in the passed list) correlation predicates (as BinaryRelationalOperatorNodes) for the subquery and also
     * removes them from the subquery where clause. The logic here is highly dependent on the shape of the where-clause
     * subtree which we assert in *SubqueryWhereVisitor.
     */
    public static ValueNode findCorrelatedSubqueryPredicates(ValueNode root,
                                                             List<BinaryRelationalOperatorNode> predToSwitch,
                                                             splice.com.google.common.base.Predicate<BinaryRelationalOperatorNode> bronPredicate) {
        if (root instanceof AndNode) {
            AndNode andNode = (AndNode) root;
            ValueNode left = findCorrelatedSubqueryPredicates(andNode.getLeftOperand(), predToSwitch, bronPredicate);
            ValueNode right = findCorrelatedSubqueryPredicates(andNode.getRightOperand(), predToSwitch, bronPredicate);
            if (left == null) {
                return right;
            } else if (right == null) {
                return left;
            }
            andNode.setLeftOperand(left);
            andNode.setRightOperand(right);
            return root;
        } else if (root instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) root;
            if (bronPredicate.apply(bron)) {
                predToSwitch.add(bron);
                return null;
            }
        }
        return root;
    }

    /**
     * Use this method to decrement the nesting level in a predicate we are moving from the where clause of a subquery
     * to the outer where clause.  Currently assumes all ColumnReferences are directly under the BRON.
     * <pre>
     *    BRON
     *    /  \
     *   CR  CR
     *   </pre>
     */
    public static void decrementColRefNestingLevel(BinaryRelationalOperatorNode bron) {
        ValueNode left = bron.getLeftOperand();
        ValueNode right = bron.getRightOperand();
        if (left instanceof ColumnReference) {
            ColumnReference leftCR = (ColumnReference) left;
            if (leftCR.getCorrelated()) {
                leftCR.setNestingLevel(leftCR.getNestingLevel() - 1);
            }
        }
        if (right instanceof ColumnReference) {
            ColumnReference rightCR = (ColumnReference) right;
            if (rightCR.getCorrelated()) {
                rightCR.setNestingLevel(rightCR.getNestingLevel() - 1);
            }
        }
    }

    /**
     * Find the ColumnReference on one side of a BRON that has the specified sourceLevel.
     */
    public static ColumnReference findColumnReference(BinaryRelationalOperatorNode bron, int sourceLevel) {
        if (bron.getLeftOperand() instanceof ColumnReference) {
            ColumnReference leftColRef = (ColumnReference) bron.getLeftOperand();
            if (leftColRef.getSourceLevel() == sourceLevel) {
                return leftColRef;
            }
        }
        if (bron.getRightOperand() instanceof ColumnReference) {
            ColumnReference rightColRef = (ColumnReference) bron.getRightOperand();
            if (rightColRef.getSourceLevel() == sourceLevel) {
                return rightColRef;
            }
        }
        throw new IllegalArgumentException("could not find ColumnReference with sourceLevel = " + sourceLevel);
    }

    /**
     * Find SelectNodes at the same level as the passed select node.  You might use this to find all SelectNodes that
     * are part of a union subquery, for example.   SubqueryNode -> SelectNode -> UnionNode -> SelectNode ...
     *
     * Will NOT traverse SubqueryNode/FromSubquery and thus only returns nodes at the same nesting level.
     */
    public static List<SelectNode> findSameLevelSelectNodes(SubqueryNode subqueryNode) throws StandardException {
        ResultSetNode subquerySelectNode = subqueryNode.getResultSet();
        return CollectingVisitorBuilder
                .forClass(SelectNode.class)
                .skipping(SubqueryNode.class)
                .skipping(FromSubquery.class)
                .collect(subquerySelectNode.getFromList());
    }

    /**
     * Find UnionNodes at the same level as the passed select node.  You might use this to find all UnionNodes that are
     * part of a union subquery, for example.   SubqueryNode -> SelectNode -> UnionNode -> SelectNode ...
     *
     * Will NOT traverse SubqueryNode/FromSubquery and thus only returns nodes at the same nesting level.
     */
    public static List<UnionNode> findSameLevelUnionNodes(SubqueryNode subqueryNode) throws StandardException {
        ResultSetNode subquerySelectNode = subqueryNode.getResultSet();
        return CollectingVisitorBuilder
                .forClass(UnionNode.class)
                .skipping(SubqueryNode.class)
                .skipping(FromSubquery.class)
                .collect(subquerySelectNode);
    }
}
