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
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import java.util.ArrayList;

/**
 * Bind an index expression.
 *
 */
public class IndexExpressionBindingVisitor implements Visitor
{
    private final Optimizable optTable;
    private final FromList fromList;
    private final SubqueryList subqList;
    private final ArrayList<AggregateNode> aggrList;

    public IndexExpressionBindingVisitor(LanguageConnectionContext lcc, Optimizable optTable) throws StandardException {
        this.optTable = optTable;
        NodeFactory nf = lcc.getLanguageConnectionFactory().getNodeFactory();
        fromList = (FromList) nf.getNode(
                C_NodeTypes.FROM_LIST,
                nf.doJoinOrderOptimization(),
                this.optTable, // this should be a FromTable (see DB-12184)
                lcc.getContextManager());
        subqList = new SubqueryList();
        aggrList = new ArrayList<AggregateNode>() {};
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(optTable instanceof FromTable)) {
            return node;
        }

        // JavaToSQLValueNode has to be bound because the subtree structure might change during binding.
        // In case of a method call, method name may be resolved to a different one.
        // For UnaryDateTimestampOperatorNode, the subtree might be evaluated to a constant.
        if (node instanceof JavaToSQLValueNode || node instanceof UnaryDateTimestampOperatorNode) {
            return ((ValueNode) node).bindExpression(fromList, subqList, aggrList);
        }
        return node;
    }

    public boolean stopTraversal()
    {
        return false;
    }

    public boolean skipChildren(Visitable node)
    {
        return false;
    }

    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }
}
