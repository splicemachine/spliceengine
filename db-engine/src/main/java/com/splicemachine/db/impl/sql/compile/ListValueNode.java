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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.Collections;
import java.util.List;

import static com.splicemachine.db.iapi.types.TypeId.LIST_ID;

/**
 * A ListValue node holds a fixed number of ValueNodes, which may be constants, parameters,
 * column references, UDFs, etc.  The order of the values in the list matters, so
 * a list such as (1, 2, 3) is not equivalent to (2, 1, 3).
 * The initial use case is to represent IN predicates such as:
 * (col1, col2) in ((1,3), (5,4), (0,55)).  All ValueNodes in one ListValueNode must
 * equal their corresponding ValueNodes in another ListValueNode for the two to be
 * equivalent.
 */

public final class ListValueNode extends ValueNode {
    
    ValueNodeList valuesList = null;
    
    public ValueNode getValue(int index) {
        if (index < 0 || index > valuesList.size())
            return null;
        return (ValueNode) valuesList.elementAt(index);
    }
    
    public int numValues() {
        return valuesList.size();
    }
    
    @Override
    public boolean isConstantExpression() {
        final int listLen = valuesList.size();
        for (int i = 0; i < listLen; i++) {
            if (!((ValueNode) valuesList.elementAt(i)).isConstantExpression())
                return false;
        }
        return true;
    }
    
    public boolean allConstantsNodesInList() {
        final int listLen = valuesList.size();
        for (int i = 0; i < listLen; i++) {
            if (!(valuesList.elementAt(i) instanceof ConstantNode))
                return false;
        }
        return true;
    }
    
    public boolean containsAllConstantNodes() {
        
        final int listLen = valuesList.size();
        for (int i = 0; i < listLen; i++) {
            if (!(valuesList.elementAt(i) instanceof ConstantNode))
                return false;
        }
        return true;
    }
    
    public int getMaximumWidth() {
        int size = valuesList.size();
        int maxWidth = 0;
        for (int index = 0; index < size; index++) {
            int tempWidth = ((ValueNode) valuesList.elementAt(index)).getTypeServices().getMaximumWidth();
            if ((long) tempWidth + (long) maxWidth > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            maxWidth += tempWidth;
        }
        return maxWidth;
    }
    /**
     * Initializer for a ListValueNode.
     *
     * @param arg1 A ListDataType containing the values of the constant.
     * @throws StandardException
     */
    public void init(Object arg1)
        throws StandardException {

        if (arg1 == null || !(arg1 instanceof ValueNodeList))
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
        
        valuesList = (ValueNodeList) arg1;
        setType(LIST_ID, valuesList.isNullable(), getMaximumWidth());
    }

    public boolean isNull() {
        for (Object vn : valuesList) {
            if (vn instanceof ConstantNode) {
                if (((ConstantNode)vn).isNull())
                    return true;
            }
        }
        return false;
    }
    
    // Push a ListDataType DVD on the stack.
    @Override
    public void generateExpression
    (
        ExpressionClassBuilder acb,
        MethodBuilder mb
    ) throws StandardException {
        /* Are we generating a SQL null value? */
        if (isNull()) {
            acb.generateNull(mb, getTypeCompiler(),
                getTypeServices());
        } else {
            // Build a new ListDataType, and place on the stack.
            int numValsInSet = this.numValues();
            LocalField dvdField = PredicateList.generateListData(acb, numValsInSet);
            ValueNode dataLiteral;
            for (int constIdx = 0; constIdx < numValsInSet; constIdx++) {
                mb.getField(dvdField);
                dataLiteral = this.getValue(constIdx);
                dataLiteral.generateExpression(acb, mb);
                mb.upCast(ClassName.DataValueDescriptor);
                mb.push(constIdx);
                mb.callMethod(VMOpcode.INVOKEVIRTUAL,
                    (String) null,
                    "setFrom",
                    "void", 2);
                
            }
            mb.getField(dvdField);
            mb.upCast(ClassName.DataValueDescriptor);
        }
    }
    
    
    /**
     * Return the value as a string.
     *
     * @return The value as a string.
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("(");
        for (int i = 0; i < valuesList.size(); i++) {
            s.append(valuesList.elementAt(i).toString());
            if (i != valuesList.size() - 1)
                s.append(", ");
        }
        s.append(")");
        return s.toString();
    }
    
    public int hashCode() {
        final int prime = 37;
        int result = 17;
        
        for (int i = 0; i < numValues(); i++) {
            result = result * prime + valuesList.elementAt(i).hashCode();
        }
        return result;
    }
    
    @Override
    public String toHTMLString() {
        return "value: " + toString() + "<br>" + super.toHTMLString();
    }
    
    
    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        
        if (valuesList != null) {
            valuesList = (ValueNodeList)valuesList.accept(v, this);
        }
    }
    
    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            ListValueNode other = (ListValueNode) o;
            
            if (valuesList == other.valuesList)
                return true;
            
            if (valuesList == null || other.valuesList == null)
                return false;
            
            if (valuesList.size() != other.valuesList.size())
                return false;
            
            for (int i = 0; i < valuesList.size(); i++) {
                if (!((ValueNode) valuesList.elementAt(i)).
                    isEquivalent((ValueNode) other.valuesList.elementAt(i)))
                    return false;
            }
            return true;
        }
        return false;
    }
    
    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);
            
            if (valuesList != null) {
                printLabel(depth, "Constants list: ");
                valuesList.treePrint(depth + 1);
            }
        }
    }
    
    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return ValueNode            The remapped expression tree.
     * @throws StandardException Thrown on error
     */
    public ValueNode remapColumnReferencesToExpressions()
        throws StandardException {
        valuesList = valuesList.remapColumnReferencesToExpressions();
        return this;
    }
    
    public List<? extends QueryTreeNode> getChildren() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public QueryTreeNode getChild(int index) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        for (Object ob:valuesList) {
            if (!((ValueNode)ob).isConstantOrParameterTreeNode())
                return false;
        }
        return true;
    }
}
