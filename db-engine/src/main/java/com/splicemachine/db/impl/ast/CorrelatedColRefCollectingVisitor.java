package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.OrderedColumn;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.VirtualColumnNode;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.base.Predicates;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by msirek on 4/14/21.
 */
public class CorrelatedColRefCollectingVisitor<T> extends ColumnCollectingVisitor {
    boolean done = false;
    private final List<T> correlatedColumns;
    private final int maxAllowableHits;
    private final int sourceLevel;

    // Collect all of the correlated ColumnReferences at the given sourceLevel.
    // If more than maxAllowableHits are found, empty out the list of found ColumnReferences
    // and end the search.
    public CorrelatedColRefCollectingVisitor(int maxAllowableHits, int sourceLevel) {
        super(Predicates.instanceOf(ColumnReference.class));
        if (maxAllowableHits < 1)
            done = true;
        this.maxAllowableHits = maxAllowableHits;
        this.sourceLevel = sourceLevel;
        correlatedColumns = new ArrayList<>();
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) {
        super.visit(node, parent);
        List<T> foundColumns = super.getCollected();
        if (!foundColumns.isEmpty()) {
            for (Object qtNode:foundColumns) {
                ColumnReference colRef = (ColumnReference)qtNode;
                if (!colRef.getCorrelated())
                    continue;
                if (colRef.getSourceLevel() != sourceLevel)
                    continue;
                if (correlatedColumns.size() == maxAllowableHits) {
                    correlatedColumns.clear();
                    done = true;
                }
                else
                    correlatedColumns.add((T)colRef);
            }
            foundColumns.clear();
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        if (done)
            return true;
        return super.skipChildren(node);
    }

    @Override
    public List<T> getCollected() {
        return correlatedColumns;
    }
}
