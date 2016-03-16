package com.splicemachine.db.impl.ast;


import org.sparkproject.guava.base.Predicate;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;

/**
 * Visitor that applies a visitor to all nodes except those identified by a predicate.
 * Skipped nodes are not traversed.
 *
 *
 * @author P Trolard
 *         Date: 30/10/2013
 */
public class SkippingVisitor implements Visitor {
    final Visitor v;
    final Predicate<? super Visitable> skip;

    public SkippingVisitor(final Visitor v, final Predicate<? super Visitable> skip){
        this.v = v;
        this.skip = skip;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (skip.apply(node)){
            return node;
        }
        return v.visit(node, parent);
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

