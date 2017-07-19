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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;


/**
 * This visitor probes the predicates looking for restrictions on the first column in a key.
 *
 */
public class ExcludedIndexColumnVisitor implements Visitor {
    int tableNumber;
    int columnNumber;
    boolean nullsExcluded;
    boolean isValid;
    DataValueDescriptor defaultValue;


    public ExcludedIndexColumnVisitor(int tableNumber, int columnNumber, boolean nullsExcluded, DataValueDescriptor defaultValue){
        this.tableNumber = tableNumber;
        this.columnNumber = columnNumber;
        this.isValid = false;
        this.nullsExcluded = nullsExcluded;
        this.defaultValue = defaultValue;
    }


    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if(node instanceof ColumnReference){
            ColumnReference cr = (ColumnReference) node;

            if(cr.getSource() != null &&
                cr.getTableNumber() == tableNumber &&
                cr.getColumnNumber() == columnNumber) {
                if (parent != null &&
                        parent.getNodeType() == C_NodeTypes.IS_NULL_NODE &&
                        nullsExcluded) {
                    // Predicate does not validate a restriction on the index.
                } else if (parent != null &&
                        parent instanceof BinaryRelationalOperatorNode && // BRO
                        ((BinaryRelationalOperatorNode) parent).getRightOperand() instanceof ConstantNode) {
                    DataValueDescriptor compare = ((ConstantNode) ((BinaryRelationalOperatorNode) parent).getRightOperand()).getValue();
                    if (compare != null && compare.equals(defaultValue)) {
                        // Predicate Does not restrict since it is doing a predicate comparison
                    } else {
                        isValid = true;
                    }
                }
                else {
                    isValid = true;
                }
            }
        }
        return node;
    }


    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }


    public boolean stopTraversal() {
        return false;
    }


    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    /**
     * The visitor collects tables found in a branch of the tree, this method
     * return true if the ResultColumn's source table has been found by the Visitor.
     * It will return false if the source table has not been found (i.e. it exists in another
     * area of the plan, such as the opposite side of the join.
     *
     * @param rc
     * @return
     */
    private boolean isValid(){
        return isValid;
    }

}
