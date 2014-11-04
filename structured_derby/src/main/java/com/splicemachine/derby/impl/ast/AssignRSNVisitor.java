package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.log4j.Logger;

/**
 * User: pjt
 * Date: 7/24/13
 */
public class AssignRSNVisitor extends AbstractSpliceVisitor {
    private static Logger LOG = Logger.getLogger(AssignRSNVisitor.class);

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {
        if (node instanceof ResultSetNode) {
            ((ResultSetNode) node).assignResultSetNumber();
        }
        return node;
    }
}