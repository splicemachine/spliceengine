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
     *     select A.* from A where a3 = (select sum(b2) from B where b1=a1);
     * </pre>
     *
     * In the above we change a3 = (subquery) to a3 = foo.sumColAlias where foo is the FromSubquery table alias.
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
     */
    public static ValueNode replaceSubqueryWithTrue(ValueNode node, FromSubquery fsq) throws StandardException {
        /* For EXISTS subqueries only it is possible for the entire where clause of the outer query to == the exist subquery. */
        if (node instanceof SubqueryNode && ((SubqueryNode) node).getResultSet() == fsq.getSubquery()) {
            return newBooleanTrue(fsq);
        } else if (node instanceof BinaryOperatorNode) {
            BinaryOperatorNode root = (BinaryOperatorNode) node;
            ValueNode left = root.getLeftOperand();
            ValueNode right = root.getRightOperand();
            if (left instanceof SubqueryNode && ((SubqueryNode) left).getResultSet() == fsq.getSubquery()) {
                root.setLeftOperand(newBooleanTrue(fsq));
                return root;
            } else if (right instanceof SubqueryNode && ((SubqueryNode) right).getResultSet() == fsq.getSubquery()) {
                root.setRightOperand(newBooleanTrue(fsq));
                return root;
            } else {
                left = replaceSubqueryWithTrue(left, fsq);
                right = replaceSubqueryWithTrue(right, fsq);
                root.setLeftOperand(left);
                root.setRightOperand(right);
                return root;
            }
        } else if (node instanceof ColumnReference) {
            return node;
        }
        return node;
    }

    private static BooleanConstantNode newBooleanTrue(FromSubquery fsq) throws StandardException {
        return (BooleanConstantNode) fsq.getNodeFactory().getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                Boolean.TRUE,
                fsq.getContextManager());
    }

}
