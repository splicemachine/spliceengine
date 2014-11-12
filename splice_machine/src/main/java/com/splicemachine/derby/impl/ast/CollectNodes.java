package com.splicemachine.derby.impl.ast;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import java.util.LinkedList;
import java.util.List;

/**
 * Collect all nodes designated by predicate. (After Derby's CollectNodesVisitor.)
 *
 * @author P Trolard
 *         Date: 31/10/2013
 */

public class CollectNodes<T> implements Visitor {
    private final List<T> nodeList;
    private final Predicate<? super Visitable> pred;

    public CollectNodes(Predicate<? super Visitable> pred) {
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


    // Builder-like constructor which allows fluent wrapping of CollectNodes visitors
    // with various modifying visitors

    public static <T> CollectNodesBuilder<T> collector(Class<T> clazz){
        return CollectNodes.collector(Predicates.instanceOf(clazz));
    }

    public static <T> CollectNodesBuilder<T> collector(Predicate<? super Visitable> pred){
        return new CollectNodesBuilder<T>(pred);
    }


    public static class CollectNodesBuilder<T> {
        private final CollectNodes<T> collector;
        private Visitor wrapped;

        public CollectNodesBuilder(Predicate<? super Visitable> pred){
            collector = new CollectNodes<T>(pred);
            wrapped = collector;
        }

        public CollectNodesBuilder<T> skipping(Predicate<? super Visitable> p){
            wrapped = new SkippingVisitor(wrapped, p);
            return this;
        }

        public CollectNodesBuilder<T> until(Predicate<? super Visitable> p){
            wrapped = new VisitUntilVisitor(wrapped, p);
            return this;
        }

        public CollectNodesBuilder<T> onAxis(Predicate<? super Visitable> p){
            wrapped = new AxisVisitor(wrapped, p);
            return this;
        }

        public List<T> collect(Visitable node) throws StandardException {
            node.accept(wrapped);
            return collector.getCollected();
        }
    }

}
