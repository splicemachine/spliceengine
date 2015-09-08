package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.google.common.collect.Iterables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.CollectNodes;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedEqualityPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.IsCorrelatedPredicate;
import org.apache.log4j.Logger;

import java.util.List;

/**
 *
 */
class ExistsSubqueryWhereVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryWhereVisitor.class);

    /* The level of the subquery we are considering flattening in the enclosing predicate */
    private final int subqueryLevel;

    private boolean foundUnsupported;
    private boolean correlatedEquality;

    public ExistsSubqueryWhereVisitor(int subqueryLevel) {
        this.subqueryLevel = subqueryLevel;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            /* do nothing */
        } else if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) node;
            if (new CorrelatedEqualityPredicate(this.subqueryLevel - 1).apply(bron)) {
                correlatedEquality = true;
            } else {
                foundUnsupported = isSubtreeCorrelated(node);
            }
        } else {
            /* Current node is not an AndNode, OrNode, or BinaryRelationalOperatorNode -- ok as long as there are no
             * correlated predicates belows us, we can move/flatten any type of uncorrelated subtree */
            foundUnsupported = node instanceof OrNode || isSubtreeCorrelated(node);
        }
        return node;
    }

    private static boolean isSubtreeCorrelated(Visitable node) throws StandardException {
        List<ColumnReference> columnReferences = CollectNodes.collector(ColumnReference.class).collect(node);
        return Iterables.any(columnReferences, new IsCorrelatedPredicate());
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        boolean isAndNode = node instanceof AndNode;
        return !isAndNode;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return foundUnsupported;
    }

    public boolean isFoundUnsupported() {
        return foundUnsupported;
    }

    public boolean isCorrelatedEquality() {
        return correlatedEquality;
    }
}