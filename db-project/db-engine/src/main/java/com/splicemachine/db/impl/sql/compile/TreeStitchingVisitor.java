package com.splicemachine.db.impl.sql.compile;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

/**
 * This visitor is used by WindowResultSetNode when breaking an existing parent/child ResultNode
 * (ProjectRestrictNode) relationship to create and insert itself into the tree.
 * <p/>
 * Given a mapping of existing child ResultColumns and new VirtualColumnNodes that point to new ResultColumns
 * in the WindowResultSetNode, it crawls parent ResultColumn expressions and finds the expressions that still
 * reference one of the given ResultColumns.  When an expression like this is found, it returns the
 * matching new VirtualColumnNode.
 */
class TreeStitchingVisitor implements Visitor {
    private Logger LOG;

    Map<ResultColumn, VirtualColumnNode> rcToNewExpression = new HashMap<>();
    private Class skipOverClass;

    TreeStitchingVisitor(Class skipThisClass, Logger LOG) {
        skipOverClass = skipThisClass;
        this.LOG = LOG;
    }

    public void addMapping(ResultColumn rc, VirtualColumnNode newVCN) {
        rcToNewExpression.put(rc, newVCN);
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ValueNode)) {
            return node;
        }

        ValueNode nd = (ValueNode) node;
        ResultColumn oldRC = null;
        String expressionName = null;
        if (nd instanceof VirtualColumnNode) {
            oldRC = ((VirtualColumnNode)nd).getSourceColumn();
            expressionName = "VirtualColumnNode: "+oldRC.getName();
        } else if (nd instanceof ColumnReference) {
            oldRC = ((ColumnReference)nd).getSource();
            expressionName = "ColumnReference: "+nd.getColumnName();
        }

        if (oldRC != null) {
            VirtualColumnNode newVCN = rcToNewExpression.get(oldRC);
            if (newVCN != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("TreeStitchingVisitor: Replacing expression "+expressionName+" on parent type: "+
                    parent.getClass().getSimpleName());
                }
                return newVCN;
            }
        }
        return node;
    }

    public boolean stopTraversal() {
        return false;
    }

    public boolean skipChildren(Visitable node) {
        return skipOverClass != null && skipOverClass.isInstance(node);
    }

    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }
}
