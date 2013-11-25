package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Visitor that applies a visitor until a stopping point defined by a predicate. Parameters of the traversal
 * defined by the wrapped visitor.
 *
 * @author P Trolard
 *         Date: 30/10/2013
 */
public class VisitUntilVisitor implements Visitor {
    private boolean stop = false;
    Visitor v;
    Predicate<? super Visitable> pred;

    public VisitUntilVisitor(final Visitor v, final Predicate<? super Visitable> pred){
        this.v = v;
        this.pred = pred;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        if (pred.apply(node)){
            stop = true;
            return node;
        }
        return v.visit(node);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return stop;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return v.skipChildren(node);
    }
}
