package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.ResultSetNode;

/**
 * User: pjt
 * Date: 7/9/13
 */
public abstract class AbstractSpliceVisitor implements ISpliceVisitor {
    String query;
    int phase;

    @Override
    public void setContext(String query, int phase) {
        this.query = query;
        this.phase = phase;
    }

    @Override
    public boolean isPostOrder() {
        // Default to PostOrder traversal, i.e. bottom-up
        return true;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public ResultSetNode visit(ResultSetNode node) {
        return node;
    }

    @Override
    public Visitable visit(Visitable node) {
       return node;
    }
}
