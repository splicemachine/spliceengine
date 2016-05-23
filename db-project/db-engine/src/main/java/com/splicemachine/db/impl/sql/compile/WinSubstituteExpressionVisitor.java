package com.splicemachine.db.impl.sql.compile;

import org.apache.log4j.Logger;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;

/**
 * Replaces an equivalent <em>source</em> expression with a <em>target</em>
 * expression.
 */
class WinSubstituteExpressionVisitor extends SubstituteExpressionVisitor {
    private ResultColumn srcRC;
    private Logger LOG;

    WinSubstituteExpressionVisitor(ValueNode s, ValueNode t, Class skipThisClass, Logger LOG) {
        super(s, t, skipThisClass);
        srcRC = source.getSourceResultColumn();
        if (LOG.isDebugEnabled()) {
            LOG.debug("SubstituteVisitor: Source RC is "+srcRC);
        }
        this.LOG = LOG;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ValueNode)) {
            return node;
        }

        ValueNode nd = (ValueNode) node;
        if (srcRC != null && nd instanceof ColumnReference && source instanceof ColumnReference) {
            // ColumnReference#isEquvalent() compares node "tableNumber"s. When pulling up column
            // references in WindowResultSetNode (pre-optimise), we don't have a tableNumber yet.
            // This behavior compares ColumnReference column descriptors.
            ResultColumn ndRC = nd.getSourceResultColumn();
            if (ndRC != null && ndRC.getTableColumnDescriptor().isEquivalent(srcRC.getTableColumnDescriptor())) {
                ++numSubstitutions;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SubstituteVisitor: Replacing (non-null source RC) "+nd);
                }
                return target;
            }
        } else if (nd instanceof ColumnReference && source instanceof ColumnReference) {
            if (source.getColumnName() != null && source.getColumnName().endsWith("Result")) {
                ResultColumn ndRC = ((ColumnReference)nd).getSource();
                if (ndRC != null && ndRC.getExpression().isEquivalent(source)) {
                    ++numSubstitutions;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SubstituteVisitor: Replacing (null source RC) "+nd);
                    }
                    return target;
                }
            }
        } else if (nd.isEquivalent(source)) {
            ++numSubstitutions;
            if (LOG.isDebugEnabled()) {
                LOG.debug("SubstituteVisitor: Replacing (generic) "+nd);
            }
            return target;
        }
        return node;
    }
}
