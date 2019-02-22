/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.LinkedList;
import java.util.List;

/**
 * Perform predicate simplification.  This should be invoked after constant
 * folding and before unsatisfiable condition tree pruning.
 *
 * This visitor eliminates a TRUE {@code BooleanConstantNode} under an AndNode,
 * replacing the AndNode with the other child of the AndNode.
 *
 * This visitor also eliminates a FALSE {@code BooleanConstantNode} under an OrNode,
 * replacing the OrNode with the other child of the OrNode.
 *
 * Redundant TRUE and FALSE nodes are also eliminated.
 *
 * This visitor walks the tree bottom-up, so that predicates simplified at lower
 * levels can affect more simplification at higher levels.
 */
class PredicateSimplificationVisitor implements Visitor {

    private final FromList fromListParam;
    private Class skipOverClass;

    public PredicateSimplificationVisitor(FromList fromListParam) {
        this.skipOverClass = null;
        this.fromListParam = fromListParam;
    }

    public PredicateSimplificationVisitor(FromList fromListParam, Class skipOverClass) {
        this.skipOverClass = skipOverClass;
        this.fromListParam = fromListParam;
    }

    private static boolean isBooleanTrue(Visitable node) {
        if (node instanceof BooleanConstantNode) {
            return ((BooleanConstantNode)node).isBooleanTrue();
        }
        return false;
    }
    private static boolean isBooleanFalse(Visitable node) {
        if (node instanceof BooleanConstantNode) {
            return ((BooleanConstantNode)node).isBooleanFalse();
        }
        return false;
    }

    // Eliminating expressions with ParameterNodes before binding can cause
    // queries to fail.  All parameters must be bound.
    // This method is to be used to perform binding before
    // pruning off the unnecessary predicate.
    private void bindExpressionsWithParametersBeforeEliminating(Visitable node) throws StandardException {
        if (node instanceof BooleanConstantNode)
            return;
        if (!hasParameterNode(node))
            return;
        if (node instanceof ValueNode) {
            ValueNode vn = (ValueNode) node;
            SubqueryList dummySubqueries =
                (SubqueryList) vn.getNodeFactory().
                    getNode(C_NodeTypes.SUBQUERY_LIST, vn.getContextManager());
            List<AggregateNode> dummyAggregates = new LinkedList<>();
            vn.bindExpression(fromListParam, dummySubqueries, dummyAggregates);
        }
    }

    private boolean hasParameterNode(Visitable node) throws StandardException {
        HasNodeVisitor visitor =
            new HasNodeVisitor(ParameterNode.class);
        node.accept(visitor);
        return visitor.hasNode();
    }

    // Strip out unnecessary predicates, and boolean constants TRUE and FALSE.
    // It's assumed this visitor is invoked before binding, so it performs
    // binding on all eliminated nodes, so errors don't occur for
    // parameterized queries.
    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            AndNode an = (AndNode)node;
            if (isBooleanTrue(an.getLeftOperand())) {
                return an.getRightOperand();
            }
            else if (isBooleanFalse(an.getLeftOperand())) {
                bindExpressionsWithParametersBeforeEliminating(an.getRightOperand());
                return an.getLeftOperand();
            }
            if (isBooleanTrue(an.getRightOperand())) {
                return an.getLeftOperand();
            }
            else if (isBooleanFalse(an.getRightOperand())) {
                bindExpressionsWithParametersBeforeEliminating(an.getLeftOperand());
                return an.getRightOperand();
            }
        } else if (node instanceof OrNode) {
            OrNode on = (OrNode)node;
            if (isBooleanTrue(on.getLeftOperand())) {
                bindExpressionsWithParametersBeforeEliminating(on.getRightOperand());
                return on.getLeftOperand();
            }
            else if (isBooleanFalse(on.getLeftOperand())) {
                return on.getRightOperand();
            }
            if (isBooleanTrue(on.getRightOperand())) {
                bindExpressionsWithParametersBeforeEliminating(on.getLeftOperand());
                return on.getRightOperand();
            }
            else if (isBooleanFalse(on.getRightOperand())) {
                return on.getLeftOperand();
            }
        }
        return node;
    }

    /**
     * {@inheritDoc}
     * @return {@code false}, since the entire tree should be visited
     */
    public boolean stopTraversal() {
        return false;
    }

    /**
     * Don't visit childen under the skipOverClass
     * node, if it isn't null.
     *
     * @return true/false
     */
    public boolean skipChildren(Visitable node)
    {
        return skipOverClass != null && skipOverClass.isInstance(node);
    }

    /**
     * {@inheritDoc}
     * @return {@code true}, since the tree should be walked bottom-up
     */
    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }

}
