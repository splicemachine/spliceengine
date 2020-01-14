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
package com.splicemachine.db.impl.sql.compile.subquery.ssq;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.FromSubqueryColRefFactory;

/**
 * Created by yxia on 10/18/17.
 */
public class ScalarSubqueryCorrelatedPredicateVisitor implements Visitor {
    private FromSubquery fromSubquery;
    private SelectNode outerSelect;
    private int nestinglvl;

    public ScalarSubqueryCorrelatedPredicateVisitor(FromSubquery subqueryNode,
                                                    int nlvl,
                                                    SelectNode selectNode) {
        fromSubquery = subqueryNode;
        outerSelect = selectNode;
        nestinglvl = nlvl;
    }
    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof ColumnReference) {
            if (((ColumnReference)node).getSourceLevel() == nestinglvl) {
                ColumnReference colRef = replaceColumnReferenceWithNewRC((ColumnReference) node, fromSubquery, outerSelect);
                return colRef;
            } else {
                ((ColumnReference)node).setNestingLevel(outerSelect.getNestingLevel());
            }
        }

        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    /** this function is used to help promote the predicate in the subquery to the outer query. To achieve that, we
     * need to project the column referenced in the predicate that is from the subquery to be projected in the RCL,
     * then replace the original column reference in the predicate with a new one that references the RC from the subquery,
     * Also we need to promote the column references' nestinglvl in the predicate to match the outer query's nestinglvl.
     *
     */
    private ColumnReference replaceColumnReferenceWithNewRC(ColumnReference  cr,
                                                            FromSubquery fromSubquery,
                                                            SelectNode topSelect) throws StandardException {
        assert fromSubquery.getSubquery() instanceof SelectNode: "Not a supported SSQ to flatten";
        SelectNode subSelect = (SelectNode) fromSubquery.getSubquery();
        ResultColumnList subSelectRCL = subSelect.getResultColumns();

        ResultColumnList subqueryRCL = fromSubquery.getResultColumns();

        /* create a new column in the subquery select node's RCL */
        ResultColumn rc = (ResultColumn) topSelect.getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                cr.getColumnName(),
                cr,
                topSelect.getContextManager());
        rc.setColumnDescriptor(null);

        if (cr.getTableNameNode() != null) {
            rc.setSourceSchemaName(cr.getTableNameNode().getSchemaName());
            rc.setSourceTableName(cr.getTableNameNode().getTableName());
        }
        rc.setReferenced();
        rc.setResultSetNumber(subSelect.getResultSetNumber());
        subSelectRCL.addResultColumn(rc);

        /* add a new column in the subquery's RCL */
        ResultColumn subqueryRC = rc.cloneMe();

        subqueryRC.setExpression((ValueNode)topSelect.getNodeFactory().getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                subSelect,
                rc,
                ReuseFactory.getInteger(subqueryRCL.size()+1),
                topSelect.getContextManager()));

        subqueryRCL.addResultColumn(subqueryRC);

        /* return an ColumnReference to the newly added column */
        ColumnReference colRef = FromSubqueryColRefFactory.build(topSelect.getNestingLevel(), fromSubquery,
                subqueryRCL.size() - 1, topSelect.getNodeFactory(), topSelect.getContextManager());

        return colRef;
    }
}
