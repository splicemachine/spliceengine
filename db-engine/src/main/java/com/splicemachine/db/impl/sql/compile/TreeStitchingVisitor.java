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
