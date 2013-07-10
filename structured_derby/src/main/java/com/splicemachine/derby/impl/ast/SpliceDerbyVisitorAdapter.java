package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.ASTVisitor;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.log4j.Logger;

/**
 * User: pjt
 * Date: 7/9/13
 */
public class SpliceDerbyVisitorAdapter implements ASTVisitor {
    private Logger LOG = Logger.getLogger(SpliceDerbyVisitorAdapter.class);

    ISpliceVisitor v;

    public SpliceDerbyVisitorAdapter(ISpliceVisitor v){
        this.v = v;
    }

    @Override
    public void initializeVisitor() throws StandardException {
    }

    @Override
    public void teardownVisitor() throws StandardException {
    }

    @Override
    public void begin(String statementText, int phase) throws StandardException {
        v.setContext(statementText, phase);
    }

    @Override
    public void end(int phase) throws StandardException {
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        if (node instanceof ResultSetNode){
            return v.visit((ResultSetNode) node);
        }
        return v.visit(node);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return v.isPostOrder();
    }

    @Override
    public boolean stopTraversal() {
        return v.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return v.skipChildren(node);
    }
}
