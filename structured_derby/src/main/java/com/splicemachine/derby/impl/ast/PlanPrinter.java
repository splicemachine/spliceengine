package com.splicemachine.derby.impl.ast;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.DMLStatementNode;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.log4j.Logger;

import java.util.List;


/**
 * @author P Trolard
 *         Date: 30/09/2013
 */
public class PlanPrinter extends AbstractSpliceVisitor {

    public static Logger LOG = Logger.getLogger(PlanPrinter.class);

    public static final String spaces = "  ";

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
        if (LOG.isInfoEnabled() &&
                node instanceof DMLStatementNode &&
                (rsn = ((DMLStatementNode) node).getResultSetNode()) != null) {
            LOG.info(String.format("Plan nodes for query <<\n\t%s\n>>\n%s",
                    query, treeToString(0, rsn)));
        }
        return node;
    }

    public static String rsnToString(ResultSetNode node) {
        return String.format("%s (#=%s)",
                JoinInfo.className.apply(node),
                node.getResultSetNumber());
    }

    public static String treeToString(int level, ResultSetNode node)
            throws StandardException {
        List<ResultSetNode> children = ColumnUtils.getChildren(node);
        String subTree = "";
        for (ResultSetNode child : Lists.reverse(children)) {
            subTree += treeToString(level + 1, child);
        }
        return String.format("%s%s\n%s",
                Strings.repeat(spaces, level),
                rsnToString(node),
                subTree);
    }
}
