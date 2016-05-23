package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

/**
 * Replace a window function node in the tree with CR that will be used to substitute
 * with the window function's result CR.
 */
public class WindowFunctionReplacementVisitor implements Visitor {
    private Class skipOverClass;
    private int resultSetNumber;
    private int level;
    private boolean canStop;

    /**
     * Construct a visitor
     *
     */
    public WindowFunctionReplacementVisitor(int resultSetNumber, int level, Class skipOverClass) {
        this.resultSetNumber = resultSetNumber;
        this.level = level;
        this.skipOverClass = skipOverClass;
    }

    ////////////////////////////////////////////////
    //
    // VISITOR INTERFACE
    //
    ////////////////////////////////////////////////

    /**
     * If we have found the target node, we are done.
     *
     * @param node the node to process
     * @return me
     */
    public Visitable visit(Visitable node, QueryTreeNode parent) {
        if (node instanceof WindowFunctionNode) {
            try {
                node = ((WindowFunctionNode)node).replaceCallWithColumnReference(resultSetNumber, level);
                canStop = node != null;
            } catch (StandardException e) {
                throw new RuntimeException("Failed to replace window function node with column reference.", e);
            }
        }
        return node;
    }

    /**
     * Stop traversal if we found the target node
     *
     * @return true/false
     */
    public boolean stopTraversal() {
        return canStop;
    }

    /**
     * Don't visit childen under the skipOverClass
     * node, if it isn't null.
     *
     * @return true/false
     */
    public boolean skipChildren(Visitable node) {
        return (skipOverClass != null) && skipOverClass.isInstance(node);
    }

    /**
     * Visit parent before children.
     */
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }
}
