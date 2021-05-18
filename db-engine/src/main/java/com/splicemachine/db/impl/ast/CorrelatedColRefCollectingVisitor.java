package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.base.Predicates;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by msirek on 4/14/21.
 */
public class CorrelatedColRefCollectingVisitor<T> extends ColumnCollectingVisitor {
    boolean done = false;
    private final List<T> subqueryColumns;
    private final List<T> correlatedOuterTableColumns;
    private final int maxAllowableHits;
    private final int sourceLevel;

    // Collect all of the correlated ColumnReferences at the given sourceLevel.
    // If more than maxAllowableHits are found, empty out the list of found ColumnReferences
    // and end the search.  All other column references found must have a nesting level
    // one greater than sourceLevel in order to guarantee that the searched predicate
    // can be fully evaluated at the level of the subquery.  Finding ColumnReferences
    // at any other level ends the search and clears out the list of found subqueryColumns.
    public CorrelatedColRefCollectingVisitor(int maxAllowableHits, int sourceLevel) {
        super(Predicates.instanceOf(ColumnReference.class));
        if (maxAllowableHits < 1)
            done = true;
        this.maxAllowableHits = maxAllowableHits;
        this.sourceLevel = sourceLevel;
        subqueryColumns = new ArrayList<>();
        correlatedOuterTableColumns = new ArrayList<>();
    }

    private void reset() {
        subqueryColumns.clear();
        correlatedOuterTableColumns.clear();
        done = true;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) {
        super.visit(node, parent);
        List<T> foundColumns = super.getCollected();
        if (!foundColumns.isEmpty()) {
            for (Object qtNode:foundColumns) {
                ColumnReference colRef = (ColumnReference)qtNode;
                if (colRef.getSourceLevel() != sourceLevel+1) {
                    // Allow at most one column reference from the outer table
                    // or inner table, but no other level.
                    if (colRef.getSourceLevel() == sourceLevel) {
                        if (correlatedOuterTableColumns.size() == maxAllowableHits) {
                            reset();
                            break;
                        }
                        else
                            correlatedOuterTableColumns.add((T)colRef);
                        continue;
                    }
                    reset();
                    break;
                }
                if (subqueryColumns.size() == maxAllowableHits) {
                    reset();
                    break;
                }
                else
                    subqueryColumns.add((T)colRef);
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
        // Must have the same number of outer table and inner table column references.
        if (subqueryColumns.size() != correlatedOuterTableColumns.size()) {
            subqueryColumns.clear();
            correlatedOuterTableColumns.clear();
        }
        return subqueryColumns;
    }

    public T popSubqueryColumn() {
        T colRef = subqueryColumns.isEmpty() ? null :
                   subqueryColumns.remove(0);
        return colRef;
    }

    public T popCorrelatedColumn() {
        T colRef = correlatedOuterTableColumns.isEmpty() ? null :
                   correlatedOuterTableColumns.remove(0);
        return colRef;
    }

    public void initialize() {
        subqueryColumns.clear();
        correlatedOuterTableColumns.clear();
        done = false;
    }

    public List<T> getCorrelatedOuterTableColumns() {
        // Must have the same number of outer table and inner table column references.
        if (subqueryColumns.size() != correlatedOuterTableColumns.size()) {
            subqueryColumns.clear();
            correlatedOuterTableColumns.clear();
        }
        return correlatedOuterTableColumns;
    }

    public T getOuterTableColumn() {
        // Must have the same number of outer table and inner table column references.
        if (subqueryColumns.size() != correlatedOuterTableColumns.size()) {
            subqueryColumns.clear();
            correlatedOuterTableColumns.clear();
        }
        if (correlatedOuterTableColumns.size() != 1)
            return null;
        return correlatedOuterTableColumns.get(0);
    }
}
