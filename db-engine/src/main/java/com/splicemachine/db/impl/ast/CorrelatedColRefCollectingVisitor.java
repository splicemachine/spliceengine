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
    // and end the search.  All other column references found must have a nesting level
    // one greater than sourceLevel in order to guarantee that the searched predicate
    // can be fully evaluated at the level of the subquery.  Finding ColumnReferences
    // at any other level ends the search and clears out the list of found correlatedColumns.
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
                if (colRef.getSourceLevel() != sourceLevel) {
                    // Allow any number of column references at the subquery
                    // level, but no other level.
                    if (colRef.getSourceLevel() == sourceLevel+1)
                        continue;
                    correlatedColumns.clear();
                    done = true;
                    break;
                }
                if (correlatedColumns.size() == maxAllowableHits) {
                    correlatedColumns.clear();
                    done = true;
                    break;
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

    public T popColumn() {
        T colRef = correlatedColumns.isEmpty() ? null :
                   correlatedColumns.remove(0);
        return colRef;
    }

    public void initialize() {
        correlatedColumns.clear();
        done = false;
    }
}
