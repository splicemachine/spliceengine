/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.splicemachine.db.shared.common.reference.SQLState.ERROR_PARSING_EXCEPTION;

/**
 * This visitor is used by WindowResultSetNode when adding a complex SQL expression to the
 * OVER clause of a window function.  It is typically passed the grandchild ResultSetNode of
 * WindowFunctionResultSet, which needs to be modified to project each base table column in the
 * expression, pulling up and building linkage of that column from the original ResultSet.
 * For example, with the following tree structure:
 *
 * WindowFunctionResultSet
 *     ProjectRestrictNode
 *         GroupByNode   <---  The ResultSet that builds the row as the window function's source.
 *             ProjectRestrictNode
 *                 ProjectRestrictNode   <---  Column originally sourced from base table.
 *                     FromBaseTable
 *
 * ... and an OVER clause expressions such as "t1.col1 - SUM(t1.col2)", column t1.col1 will be pulled
 * up from the bottommost ProjectRestrictNode, up to the GroupByNode.
 * <p/>
 */
class ColumnPullupVisitor implements Visitor {
    private Logger LOG;

    private final Map<ResultColumn, ResultColumn> rcToNewExpression = new HashMap<>();
    private final Class skipOverClass;
    private final ResultSetNode rsn;
    private final WindowResultSetNode.RCtoCRfactory rCtoCRfactory;

    ColumnPullupVisitor(Class skipThisClass, Logger LOG, ResultSetNode rsn,
                        WindowResultSetNode.RCtoCRfactory rCtoCRfactory) {
        skipOverClass = skipThisClass;
        this.LOG = LOG;
        this.rsn = rsn;
        this.rCtoCRfactory = rCtoCRfactory;
    }

    private static ResultColumn recursivefindOrPullUpRC(ResultColumn rc, ResultSetNode currentNode,
                                                        WindowResultSetNode.RCtoCRfactory rCtoCRfactory) throws StandardException{
        ResultColumn newRC = null;
        newRC = WindowResultSetNode.findOrPullUpRC(rc, currentNode, rCtoCRfactory);
        if (newRC == null) {
            if (currentNode instanceof SingleChildResultSetNode) {
                newRC = recursivefindOrPullUpRC(rc, ((SingleChildResultSetNode) currentNode).
                                                                                getChildResult(),
                                                                                rCtoCRfactory);

                newRC = WindowResultSetNode.findOrPullUpRC(newRC, currentNode, rCtoCRfactory);
                if (newRC == null)
                    throw StandardException.newException(ERROR_PARSING_EXCEPTION,
                    "Internal error parsing expression in window function.");
            }
            else if (currentNode instanceof JoinNode) {
                newRC = recursivefindOrPullUpRC(rc, ((JoinNode) currentNode).
                                                getLeftResultSet(), rCtoCRfactory);

                newRC = WindowResultSetNode.findOrPullUpRC(newRC, currentNode, rCtoCRfactory);
                if (newRC == null) {
                    newRC = recursivefindOrPullUpRC(rc, ((JoinNode) currentNode).
                                                    getRightResultSet(), rCtoCRfactory);

                    newRC = WindowResultSetNode.findOrPullUpRC(newRC, currentNode, rCtoCRfactory);
                }
                if (newRC == null)
                    throw StandardException.newException(ERROR_PARSING_EXCEPTION,
                    "Internal error parsing expression in window function.");
            }
        }
        return newRC;

    }
    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ColumnReference)) {
            return node;
        }

        ColumnReference cr = (ColumnReference) node;
        ResultColumn oldRC = null;
        ResultColumn newRC = null;

        if (cr.getSource() == null)
            return node;

        oldRC = cr.getSource();

        // Any references to VirtualColumnNodes have not been pulled up yet.
        // VirtualColumnNode references should only be done in the ProjectRestrictNode
        // immediately above the FromBaseTable ResultSet.
        if (oldRC.getExpression() instanceof VirtualColumnNode) {
            newRC = rcToNewExpression.get(oldRC);
            if (newRC != null) {
                cr.setSource(newRC);
                return node;
            }
            newRC = recursivefindOrPullUpRC(oldRC, this.rsn, rCtoCRfactory);
            if (newRC == null)
                throw StandardException.newException(ERROR_PARSING_EXCEPTION,
                                                     "Internal error parsing expression in window function.");
            if (LOG.isDebugEnabled() && newRC != oldRC) {
                LOG.debug("ColumnPullupVisitor: Replacing ResultColumnNode: "+oldRC+" with: "+newRC);
            }
            rcToNewExpression.put(oldRC, newRC);
            cr.setSource(newRC);
            return node;
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
