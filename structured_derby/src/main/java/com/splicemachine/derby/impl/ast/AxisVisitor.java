package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Visitor that applies a visitor along a traversal "axis" defined by a predicate. Only
 * nodes that pass the predicate are considered axes along which to traverse, or, in other
 * words, are considered branches.
 *
 * @author P Trolard
 *         Date: 26/02/2014
 */
public class AxisVisitor implements Visitor {
    final Visitor v;
    final Predicate<? super Visitable> onAxis;

    public AxisVisitor(final Visitor v, final Predicate<? super Visitable> onAxis) {
        this.v = v;
        this.onAxis = onAxis;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        return v.visit(node);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return v.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return !onAxis.apply(node) || v.skipChildren(node);
    }
}
