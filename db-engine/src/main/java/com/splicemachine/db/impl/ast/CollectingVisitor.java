package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.sparkproject.guava.base.Predicate;
import org.sparkproject.guava.base.Predicates;
import java.util.LinkedList;
import java.util.List;

/**
 * Collect all nodes designated by predicates. Can specify predicate for each visited node and (optionally) for each
 * visited node's parent.
 *
 * The predicate for parent nodes should handle null if used in a context where the root node might be visited.
 */
public class CollectingVisitor<T> implements Visitor {

    private final Predicate<? super Visitable> nodePred;
    private final Predicate<? super Visitable> parentPred;

    private final List<T> nodeList;

    /**
     * Constructor: predicate on node and parent node
     */
    public CollectingVisitor(Predicate<? super Visitable> nodePred, Predicate<? super Visitable> parentPred) {
        this.nodePred = nodePred;
        this.parentPred = parentPred;
        this.nodeList = new LinkedList<>();
    }

    /**
     * Constructor: predicate on node only
     */
    public CollectingVisitor(Predicate<? super Visitable> nodePred) {
        this(nodePred, Predicates.<Visitable>alwaysTrue());
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) {
        if (nodePred.apply(node) && parentPred.apply(parent)) {
            nodeList.add((T) node);
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    public List<T> getCollected() {
        return nodeList;
    }

}