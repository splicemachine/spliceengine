package com.splicemachine.db.impl.sql.compile.subquery;

import com.google.common.base.Predicate;
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
                                          OperatorNode pred) throws StandardException {
        if (node instanceof AndNode) {
            AndNode root = (AndNode) node;
            if (root.getLeftOperand() instanceof BinaryRelationalOperatorNode) {
                return newAndNode(node, pred, root);
            } else {
                root.setLeftOperand(addPredToTree(root.getLeftOperand(), pred));
                return root;
            }

        } else {
            return newAndNode(node, pred, node);
        }
    }

    private static ValueNode newAndNode(ValueNode node, OperatorNode pred, ValueNode root) throws StandardException {
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
                                                             Predicate<BinaryRelationalOperatorNode> bronPredicate) {
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