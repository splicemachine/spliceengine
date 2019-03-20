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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import org.apache.log4j.Logger;

/**
 * Created by yxia on 3/20/19.
 */
public class RecursiveViewReferenceVisitor implements Visitor {
    private static Logger LOG = Logger.getLogger(RecursiveViewReferenceVisitor.class);
    private TableName recursiveViewName;
    private ResultSetNode subquery;
    private int numReferences = 0;

    public RecursiveViewReferenceVisitor(TableName recursiveViewName, ResultSetNode subquery) {
        this.recursiveViewName = recursiveViewName;
        this.subquery = subquery;
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
        // check the from clause of a select node and replace
        if (node != null && node instanceof SelectNode) {
            FromList fromList = ((SelectNode) node).getFromList();

            for (int i=0; i< fromList.size(); i++) {
                FromTable fromTable=(FromTable)fromList.elementAt(i);
                if (fromTable instanceof FromBaseTable && ((FromBaseTable) fromTable).tableName.equals(recursiveViewName)) {
                    numReferences ++;
                    SelfReferenceNode selfReferenceNode = (SelfReferenceNode)((SelectNode)node).getNodeFactory().getNode(
                            C_NodeTypes.FROM_SUBQUERY_HOLDER,
                            subquery,
                            recursiveViewName.getTableName(),
                            null,
                            ((SelectNode)node).getContextManager());

                    fromList.setElementAt(selfReferenceNode, i);
                }
            }
        }

        return node;
    }

    public int getNumReferences() {
        return numReferences;
    }
}
