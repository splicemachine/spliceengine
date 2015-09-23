package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.impl.sql.compile.AndNode;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;

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
                                          BinaryRelationalOperatorNode pred) throws StandardException {
        if (node instanceof AndNode) {
            AndNode root = (AndNode) node;
            if (root.getLeftOperand() instanceof BinaryRelationalOperatorNode) {
                return (AndNode) node.getNodeFactory().getNode(C_NodeTypes.AND_NODE,
                        root,
                        pred,
                        node.getContextManager());
            } else {
                root.setLeftOperand(addPredToTree(root.getLeftOperand(), pred));
                return root;
            }

        } else {
            return (AndNode) node.getNodeFactory().getNode(C_NodeTypes.AND_NODE,
                    node,
                    pred,
                    node.getContextManager());
        }
    }

    /**
     * Collects (in the passed list) correlation predicates (as BinaryRelationalOperatorNodes) for the subquery and also
     * removes them from the subquery where clause. The logic here is highly dependent on the shape of the where-clause
     * subtree which we assert in AggregateSubqueryWhereVisitor.
     */
    public static ValueNode findCorrelatedSubqueryPredicates(ValueNode root,
                                                             List<BinaryRelationalOperatorNode> predToSwitch,
                                                             int level) {
        if (root instanceof AndNode) {
            AndNode andNode = (AndNode) root;
            ValueNode left = findCorrelatedSubqueryPredicates(andNode.getLeftOperand(), predToSwitch, level);
            ValueNode right = findCorrelatedSubqueryPredicates(andNode.getRightOperand(), predToSwitch, level);
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
            if (new CorrelatedEqualityPredicate(level).apply(bron)) {
                predToSwitch.add(bron);
                return null;
            }
        }
        return root;
    }
}
