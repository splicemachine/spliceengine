package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

/**
 * Created by yifuma on 8/5/15.
 */
public class HasOuterCRVisitor implements Visitor
{
    private boolean hasCorrelatedCRs;
    private int level;

    public HasOuterCRVisitor(int level){
        this.level = level;
    }

    ////////////////////////////////////////////////
    //
    // VISITOR INTERFACE
    //
    ////////////////////////////////////////////////

    /**
     * If we have found the target node, we are done.
     *
     * @param node 	the node to process
     *
     * @return me
     */
    public Visitable visit(Visitable node)
    {
        if (node instanceof ColumnReference)
        {
            if (((ColumnReference)node).getSourceLevel() < level)
            {
                hasCorrelatedCRs = true;
            }
        }
        else if (node instanceof VirtualColumnNode)
        {
            if (((VirtualColumnNode)node).getCorrelated())
            {
                hasCorrelatedCRs = true;
            }
        }
        else if (node instanceof MethodCallNode)
        {
			/* trigger action references are correlated
			 */
            if (((MethodCallNode)node).getMethodName().equals("getTriggerExecutionContext") ||
//				((MethodCallNode)node).getMethodName().equals("TriggerOldTransitionRows") ||
                    ((MethodCallNode)node).getMethodName().equals("TriggerNewTransitionRows")
                    )
            {
                hasCorrelatedCRs = true;
            }
        }
        return node;
    }

    /**
     * Stop traversal if we found the target node
     *
     * @return true/false
     */
    public boolean stopTraversal()
    {
        return hasCorrelatedCRs;
    }

    public boolean skipChildren(Visitable v)
    {
        return false;
    }

    public boolean visitChildrenFirst(Visitable v)
    {
        return false;
    }

    ////////////////////////////////////////////////
    //
    // CLASS INTERFACE
    //
    ////////////////////////////////////////////////
    /**
     * Indicate whether we found the node in
     * question
     *
     * @return true/false
     */
    public boolean hasCorrelatedCRs()
    {
        return hasCorrelatedCRs;
    }


}