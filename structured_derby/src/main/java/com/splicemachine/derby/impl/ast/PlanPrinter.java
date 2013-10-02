package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.DMLStatementNode;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.log4j.Logger;


/**
 * @author P Trolard
 *         Date: 30/09/2013
 */
public class PlanPrinter extends AbstractSpliceVisitor {

    public static Logger LOG = Logger.getLogger(PlanPrinter.class);

    // Only visit root node

    @Override
    public boolean isPostOrder() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return true;
    }

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {
        ResultSetNode rsn;
        if (node instanceof DMLStatementNode &&
                (rsn = ((DMLStatementNode)node).getResultSetNode()) != null) {
            Iterable names = JoinSelector.classNames(JoinSelector.getSelfAndChildren(rsn));
            LOG.error(String.format("Plan nodes for query \n\t%s\n\n\t%s", query, names));
        }
        return node;
    }
}
