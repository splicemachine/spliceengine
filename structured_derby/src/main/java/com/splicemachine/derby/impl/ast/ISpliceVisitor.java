package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.ResultSetNode;

/**
 * User: pjt
 * Date: 7/9/13
 */
public interface ISpliceVisitor {
    public void setContext(String query, int phase);
    public boolean isPostOrder();
    public boolean stopTraversal();
    public boolean skipChildren(Visitable node);

    public ResultSetNode visit(ResultSetNode node);
    public Visitable visit(Visitable node);
}
