package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Visitor that applies a visitor to all nodes except those identified by a predicate.
 * Skipped nodes are not traversed.
 *
 *
 * @author P Trolard
 *         Date: 30/10/2013
 */
public class SkippingVisitor implements Visitor {
    private boolean stop = false;
    Visitor v;
    Predicate<Visitable> skip;

    public SkippingVisitor(final Visitor v, final Predicate<Visitable> skip){
        this.v = v;
        this.skip = skip;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        if (skip.apply(node)){
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
        return v.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return skip.apply(node) || v.skipChildren(node);
    }
}

