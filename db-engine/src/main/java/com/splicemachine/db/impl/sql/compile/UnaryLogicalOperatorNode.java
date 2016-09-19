/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.util.List;

public abstract class UnaryLogicalOperatorNode extends UnaryOperatorNode{
    /**
     * Initializer for a UnaryLogicalOperatorNode
     *
     * @param operand    The operand of the operator
     * @param methodName The name of the method to call in the generated
     *                   class.  In this case, it's actually an operator
     *                   name.
     */
    @Override
    public void init(Object operand,Object methodName){
        /* For logical operators, the operator and method names are the same */
        super.init(operand,methodName,methodName);
    }

    /**
     * Bind this logical operator.  All that has to be done for binding
     * a logical operator is to bind the operand, check that the operand
     * is SQLBoolean, and set the result type to SQLBoolean.
     *
     * @param fromList        The query's FROM list
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     * @return The new top of the expression tree.
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException{
        bindOperand(fromList,subqueryList, aggregateVector);

		/*
		** Logical operators work only on booleans.  If the operand 
		** is not boolean, throw an exception.
		**
		** For now, this exception will never happen, because the grammar
		** does not allow arbitrary expressions with NOT.  But when
		** we start allowing generalized boolean expressions, we will modify
		** the grammar, so this test will become useful.
		*/

        if(!operand.getTypeServices().getTypeId().isBooleanTypeId()){
            throw StandardException.newException(SQLState.LANG_UNARY_LOGICAL_NON_BOOLEAN);
        }

		/* Set the type info */
        setFullTypeInfo();

        return this;
    }

    /**
     * Set all of the type info (nullability and DataTypeServices) for
     * this node.  Extracts out tasks that must be done by both bind()
     * and post-bind() AndNode generation.
     *
     * @throws StandardException Thrown on error
     */
    protected void setFullTypeInfo() throws StandardException{
        boolean nullableResult;

		/*
		** Set the result type of this comparison operator based on the
		** operands.  The result type is always SQLBoolean - the only question
		** is whether it is nullable or not.  If either of the operands is
		** nullable, the result of the comparison must be nullable, too, so
		** we can represent the unknown truth value.
		*/
        nullableResult=operand.getTypeServices().isNullable();
        setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID,nullableResult));
    }
}
