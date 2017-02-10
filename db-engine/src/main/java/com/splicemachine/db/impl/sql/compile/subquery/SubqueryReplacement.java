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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.impl.sql.compile.*;

/**
 * Shared logic that removes the subquery node from the three as part of flattening.
 */
public class SubqueryReplacement {

    /**
     * This replaces the subquery in the outer query with a column reference to the new FromSubquery.
     *
     * EXAMPLE:
     * <pre>
     *     FROM: where a1 = 5 and a3 = (select sum(b2) from B where b1=a1);
     *     TO  : where a1 = 5 and a3 = fromSubqueryTableAlias.r
     * </pre>
     *
     * We know which subquery node to replace because we have a reference to it in the passed FromSubquery parameter.
     *
     * @param node  this is initially the where clause of the outer query.
     * @param fsq   the new FromSubquery that is created in the context of flattening
     * @param level the nesting level of the outer query
     */
    public static ValueNode replaceSubqueryWithColRef(ValueNode node,
                                                      FromSubquery fsq,
                                                      int level) throws StandardException {

        if (node instanceof BinaryOperatorNode) {
            BinaryOperatorNode root = (BinaryOperatorNode) node;
            ValueNode left = root.getLeftOperand();
            ValueNode right = root.getRightOperand();
            ColumnReference colRef = FromSubqueryColRefFactory.build(level, fsq, 0, fsq.getNodeFactory(), fsq.getContextManager());

            if (left instanceof SubqueryNode && ((SubqueryNode) left).getResultSet() == fsq.getSubquery()) {
                root.setLeftOperand(colRef);
                return root;
            } else if (right instanceof SubqueryNode && ((SubqueryNode) right).getResultSet() == fsq.getSubquery()) {
                root.setRightOperand(colRef);
                return root;
            } else {
                left = replaceSubqueryWithColRef(left, fsq, level);
                right = replaceSubqueryWithColRef(right, fsq, level);
                root.setLeftOperand(left);
                root.setRightOperand(right);
                return root;
            }
        } else if (node instanceof ColumnReference) {
            return node;
        }
        return node;
    }

    /**
     * Same logic as above, except here the subquery is replaced with a boolean constant TRUE. Intended to be used to
     * replace EXISTS subqueries.
     *
     * <pre>
     *     FROM: where a1 = 5 and exists (select sum(b2) from B where b1=a1);
     *     TO  : where a1 = 5 and true;
     * </pre>
     *
     */
    public static ValueNode replaceSubqueryWithTrue(ValueNode node, SubqueryNode nodeToReplace) throws StandardException {
        /* For EXISTS subqueries only it is possible for the entire where clause of the outer query to == the exist subquery. */
        if (node instanceof SubqueryNode && node == nodeToReplace) {
            return newBooleanTrue(node);
        } else if (node instanceof BinaryOperatorNode) {
            BinaryOperatorNode root = (BinaryOperatorNode) node;
            ValueNode left = root.getLeftOperand();
            ValueNode right = root.getRightOperand();
            if (left instanceof SubqueryNode && left == nodeToReplace) {
                root.setLeftOperand(newBooleanTrue(node));
                return root;
            } else if (right instanceof SubqueryNode && right == nodeToReplace) {
                root.setRightOperand(newBooleanTrue(node));
                return root;
            } else {
                left = replaceSubqueryWithTrue(left, nodeToReplace);
                right = replaceSubqueryWithTrue(right, nodeToReplace);
                root.setLeftOperand(left);
                root.setRightOperand(right);
                return root;
            }
        } else if (node instanceof ColumnReference) {
            return node;
        }
        return node;
    }

    private static BooleanConstantNode newBooleanTrue(QueryTreeNode anyNode) throws StandardException {
        return (BooleanConstantNode) anyNode.getNodeFactory().getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                Boolean.TRUE,
                anyNode.getContextManager());
    }

}
