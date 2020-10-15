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
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.util.List;

/**
 * This node represents scalar min/max function which returns the minimum or maximum value or null.
 * The arguments are evaluated in the order in which they are specified, and the result of the function
 * is the minimum value or maximum value or null. The result is null if any argument is null. The
 * selected argument is converted, if necessary, to the attributes of the result.
 */

public class ScalarMinMaxFunctionNode extends MultiaryFunctionNode
{
    public void init(Object functionName, Object argumentsList) {
        super.init(functionName, argumentsList);
    }

    /**
     * Binding this expression means setting the result DataTypeServices.
     * In this case, the result type is based on the rules in the table listed earlier.
     *
     * @param fromList            The FROM list for the statement.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes.
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        super.bindExpression(fromList, subqueryList, aggregateVector);

        // scalar min and max do not work on LOB, XML, array, cursor, and structured type
        for (int i = 0; i < argumentsList.size(); i++) {
            if (((ValueNode) argumentsList.elementAt(i)).requiresTypeFromContext())
                continue;
            TypeId typeId = ((ValueNode) argumentsList.elementAt(i)).getTypeId();
            if (typeId.isLOBTypeId() || typeId.isXMLTypeId() || typeId.isArray()) {
                throw StandardException.newException(SQLState.LANG_UNSUPPORTED_TYPE_FOR_SCALAR_MIN_MAX);
            }
        }
        return this;
    }

    /**
     * Do code generation for scalar min/max function.
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the expression will go into
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
            throws StandardException
    {
        super.generateExpression(acb, mb, functionName.toLowerCase());
    }
}
