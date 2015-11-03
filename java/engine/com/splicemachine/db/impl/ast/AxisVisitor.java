package com.splicemachine.db.impl.ast;

import com.google.common.base.Predicate;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;

/**
 * Visitor that applies a visitor along a traversal "axis" defined by a predicate. Only nodes that pass the predicate
 * are considered axes along which to traverse, or, in other words, are considered branches.
 *
 * @author P Trolard Date: 26/02/2014
 */
public class AxisVisitor implements Visitor {

    private final Visitor delegateVisitor;

    /* Only visit children for which this predicate evaluates to true */
    private final Predicate<? super Visitable> onAxisPredicate;

    public AxisVisitor(final Visitor v, final Predicate<? super Visitable> onAxisPredicate) {
        this.delegateVisitor = v;
        this.onAxisPredicate = onAxisPredicate;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        return delegateVisitor.visit(node, parent);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return delegateVisitor.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return delegateVisitor.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        boolean shouldVisitChildren = onAxisPredicate.apply(node);
        return !shouldVisitChildren || delegateVisitor.skipChildren(node);
    }
}
