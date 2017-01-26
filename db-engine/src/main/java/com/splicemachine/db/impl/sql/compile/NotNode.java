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

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.reference.ClassName;


import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;

/**
 * A NotNode represents a NOT operator. Preprocessing will eliminate the 
 * NotNodes which exist above comparison operators so that the optimizer
 * will see a query tree in CNF.
 *
 */

public final class NotNode extends UnaryLogicalOperatorNode
{
	/**
	 * Initializer for a NotNode
	 *
	 * @param operand	The operand of the NOT
	 */

	public void init(Object operand)
	{
		super.init(operand, "not");
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		return operand.eliminateNots(! underNotNode);
	}

	/**
	 * Do code generation for the NOT operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/*
		** This generates the following code:
		**
		** <boolean field> = <operand>.equals(<operand>,
		**					 					<false truth value>);
		*/

		/*
		** Generate the code for a Boolean false constant value.
		*/
		String interfaceName = getTypeCompiler().interfaceName();
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, interfaceName);
		/*
		** Generate the call to the equals method.
		** equals is only on Orderable, not any subinterfaces.
		*/

		/* Generate the code for operand */
		operand.generateExpression(acb, mb);
		mb.upCast(ClassName.DataValueDescriptor);

		mb.dup(); // arg 1 is instance

		// arg 2
		mb.push(false);
		acb.generateDataValue(mb, getTypeCompiler(), 
				getTypeServices().getCollationType(), field);
		mb.upCast(ClassName.DataValueDescriptor);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "equals", interfaceName, 2);
	}
}
