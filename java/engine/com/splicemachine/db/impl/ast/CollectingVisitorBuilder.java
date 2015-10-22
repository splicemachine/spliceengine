package com.splicemachine.db.impl.ast;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.List;

/**
 * Build a CollectingVisitor and optionally wrap it in additional visitors.
 *
 * Use this when you want to visit all nodes in a tree and collect those that evaluate to true for a specified guava
 * predicate.
 */
public class CollectingVisitorBuilder<T> {

    private final CollectingVisitor<T> collector;
    private Visitor wrapped;

    private CollectingVisitorBuilder(Predicate<? super Visitable> nodePred,  Predicate<? super Visitable> parentPred) {
        collector = new CollectingVisitor<>(nodePred, parentPred);
        wrapped = collector;
    }

    /**
     * Terminal method in the fluent API.  Invokes the built Visitor and returns collected nodes.
     */
    public List<T> collect(Visitable node) throws StandardException {
        node.accept(wrapped);
        return collector.getCollected();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // methods for wrapping CollectingVisitor to further refine visit path
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public CollectingVisitorBuilder<T> skipping(Predicate<? super Visitable> p) {
        wrapped = new SkippingVisitor(wrapped, p);
        return this;
    }

    public CollectingVisitorBuilder<T> skipping(Class<?> clazz) {
        wrapped = new SkippingVisitor(wrapped, Predicates.instanceOf(clazz));
        return this;
    }

    public CollectingVisitorBuilder<T> until(Predicate<? super Visitable> p) {
        wrapped = new VisitUntilVisitor(wrapped, p);
        return this;
    }

    public CollectingVisitorBuilder<T> onAxis(Predicate<? super Visitable> p) {
        wrapped = new AxisVisitor(wrapped, p);
        return this;
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // static factory methods for building this builder.
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static <T> CollectingVisitorBuilder<T> forClass(Class<T> clazz) {
        return new CollectingVisitorBuilder<>(Predicates.instanceOf(clazz), Predicates.<Visitable>alwaysTrue());
    }

    public static <T> CollectingVisitorBuilder<T> forPredicate(Predicate<? super Visitable> pred) {
        return new CollectingVisitorBuilder<>(pred, Predicates.<Visitable>alwaysTrue());
    }
}
