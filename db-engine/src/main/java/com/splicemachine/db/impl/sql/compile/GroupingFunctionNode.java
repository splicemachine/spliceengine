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

/**
 * This node represents a grouping function for OLAP operations
 *
 */

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.sql.Types;
import java.util.List;

/**
 * Created by yxia on 1/16/19.
 */
public final class GroupingFunctionNode extends UnaryOperatorNode {
    private int groupByColumnPosition = -1;
    private ValueNode groupingIdRef = null;
    private ValueNode groupingIdRefForSpark = null;

    public void init(Object	operand)
    {
        super.init( operand, "grouping", "isGrouping");
    }

    public ValueNode getGroupingIdRefForSpark() {
        return groupingIdRefForSpark;
    }
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        bindOperand( fromList, subqueryList, aggregateVector);

        setType( new DataTypeDescriptor( TypeId.getBuiltInTypeId( Types.TINYINT),
                false));
        return this;
    }

    public void setGroupByColumnPosition(GroupByList groupByList) {
        if (groupByColumnPosition == -1) {
            for (int i=0; i<groupByList.size(); i++) {
                GroupByColumn groupByColumn = (GroupByColumn)groupByList.elementAt(groupByList.size()- 1 - i);
                ValueNode columnExpression = groupByColumn.getColumnExpression();
                if (columnExpression instanceof ColumnReference) {
                    ResultColumn rc = ((ColumnReference) columnExpression).getSource();
                    if (rc != null) {
                        if (operand instanceof ColumnReference &&
                            rc == ((ColumnReference)operand).getSource()) {
                            this.groupByColumnPosition = i;
                            break;
                        }
                        if (operand instanceof VirtualColumnNode &&
                            ((VirtualColumnNode)operand).getSourceColumn().getExpression() instanceof ColumnReference &&
                            rc == ((ColumnReference) ((VirtualColumnNode)operand).getSourceColumn().getExpression()).getSource()) {
                            this.groupByColumnPosition = i;
                            break;
                        }
                    }
                }
            }
        }
    }

    public void setGroupingIdRef(VirtualColumnNode vc) throws StandardException {
        if (groupingIdRef != null)
            return;

        groupingIdRef = vc;

        return;
    }

    public void setGroupingIdRefForSpark(ValueNode vn) {
        if (groupingIdRefForSpark != null)
            return;

        groupingIdRefForSpark = vn;

        return;
    }

    @Override
    public String toString() {
        return "GROUPING(" + operand.toString() + ")";
    }

    public boolean isSingleColumnExpression() {
        return operand != null && (operand instanceof ColumnReference);
    }

    public String getReceiverInterfaceName() {
        return ClassName.ConcatableDataValue;
    }

    public int getGroupByColumnPosition() {
        return groupByColumnPosition;
    }
    @Override
    public void generateExpression(ExpressionClassBuilder acb,
                                   MethodBuilder mb)
            throws StandardException {
        if (groupingIdRef == null)
            return;

        String resultTypeName = getTypeCompiler().interfaceName();

        mb.pushThis();
        groupingIdRef.generateExpression(acb, mb);
        mb.upCast( ClassName.DataValueDescriptor);
        if (groupingIdRefForSpark != null) {
            groupingIdRefForSpark.generateExpression(acb, mb);
            mb.upCast(ClassName.DataValueDescriptor);
        }
        else
            mb.pushNull(DataValueDescriptor.class.getName());

        mb.push(groupByColumnPosition);

        mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, methodName, resultTypeName, 3);
        return;

    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (groupingIdRef  != null) {
            groupingIdRef  = (ValueNode)groupingIdRef .accept(v, this);
        }
        if (groupingIdRefForSpark  != null) {
            groupingIdRefForSpark  = (ValueNode)groupingIdRefForSpark.accept(v, this);
        }
    }

}
