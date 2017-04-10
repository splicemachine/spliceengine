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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.aggregatepush;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.ResultColumn;
import com.splicemachine.db.impl.sql.compile.ValueNode;
import java.util.Map;
import java.util.HashMap;
/**
 * Created by yxia on 4/5/17.
 */
public class ColumnMapVisitor extends AbstractSpliceVisitor implements Visitor {
    HashMap<ColumnReference,ResultColumn> fieldMap;
    int sourceTableNumber;
    int targetTableNumber;

    public ColumnMapVisitor(HashMap<ColumnReference, ResultColumn> map, int sTableNum, int tTableNum) {
        fieldMap = map;
        sourceTableNumber = sTableNum;
        targetTableNumber = tTableNum;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ColumnReference) || ((ColumnReference) node).getTableNumber() != sourceTableNumber)
            return node;

        ResultColumn mappedNode = findMatch((ColumnReference)node);
        if (mappedNode != null) {
            //replace the node's source with the mapped node
            ((ColumnReference) node).setSource(mappedNode);
            ((ColumnReference) node).setTableNumber(targetTableNumber);
            ((ColumnReference) node).setColumnNumber(mappedNode.getVirtualColumnId());
            //nestingLevel and sourceNestingLevel does not need to change
            return node;
        }

        return node;
    }

    private ResultColumn findMatch(ColumnReference node)throws StandardException {
        for (Map.Entry<ColumnReference, ResultColumn> entry: fieldMap.entrySet()) {
            if (entry.getKey().isEquivalent(node))
                return entry.getValue();
        }
        return null;
    }
}
