package com.splicemachine.derby.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;

import java.util.LinkedList;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 07/10/2013
 */
public class CollectChildrenVisitor implements Visitor {

    Visitable parent;
    List<ResultSetNode> children;

    public CollectChildrenVisitor(){
        children = new LinkedList<ResultSetNode>();
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        if (node instanceof ResultSetNode) {
            if (parent == null){
                parent = node;
            } else {
                children.add((ResultSetNode)node);
            }
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return parent != null && node != parent;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    public List<ResultSetNode> getChildren(){
        return children;
    }

}
