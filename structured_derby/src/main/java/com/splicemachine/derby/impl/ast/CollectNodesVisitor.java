package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import java.util.LinkedList;
import java.util.List;

/**
 * Collect all nodes designated by predicate. (After Derby's class of same name.)
 *
 * @author P Trolard
 *         Date: 31/10/2013
 */

public class CollectNodesVisitor<T> implements Visitor {
    private final List<T> nodeList;
    private final Predicate<? super Visitable> pred;

    public CollectNodesVisitor(Predicate<? super Visitable> pred) {
        this.pred = pred;
        nodeList = new LinkedList<T>();
    }

    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    public boolean stopTraversal() {
        return false;
    }

    public Visitable visit(Visitable node) {
        if (pred.apply(node)) {
            nodeList.add((T) node);
        }
        return node;
    }

    public boolean skipChildren(Visitable node) {
        return false;
    }

    public List<T> getCollected() {
        return nodeList;
    }
}
