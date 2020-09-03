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
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.CodeGeneration;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;

import java.lang.reflect.Modifier;

public class ExecutableIndexExpressionClassBuilder extends ExpressionClassBuilder
{
	/**
	 * By the time this is done, it has constructed the following class:
	 * <pre>
	 *    public class #className extends BaseExecutableIndexExpression {
	 *	    public #className() {
	 *	      super();
	 *	      TO BE GENERATED...
	 *	    }
	 *
	 *      public void runExpression(ExecRow, ExecIndexRow) throws StandardException {
	 *        TO BE GENERATED...
	 *      }
	 *    }
	 * </pre>
	 *
	 * @exception StandardException thrown on failure
	 */
	ExecutableIndexExpressionClassBuilder(CompilerContext cc)
		throws StandardException
	{
		super(ClassName.BaseExecutableIndexExpression, null, cc);
		executeMethod = beginRunExpressionMethod();
	}

	/**
	 * Get the name of the package that the generated class will live in.
	 *
	 *	@return	name of package that the generated class will live in.
	 */
	public String getPackageName() {
		return CodeGeneration.GENERATED_PACKAGE_PREFIX;
	}

	/**
	 * Get the number of ExecRows that must be allocated
	 *
	 *	@return	number of ExecRows that must be allocated
	 */
	public int getRowCount() { return 0; }

	/**
	 * Sets the number of subqueries under this expression
	 */
	public void setNumSubqueries() throws StandardException {}

    public void setDataSetProcessorType(DataSetProcessorType type)
         throws StandardException
	{}

	/**
		Return the base class of the activation's hierarchy
		(the subclass of Object).

		This class is expected to hold methods used by all
		compilation code, such as datatype compilation code,
		e.g. getDataValueFactory.
	 */
	public String getBaseClassName() {
		return ClassName.BaseExecutableIndexExpression;
	}

	/**
	 * Generate a reference to a colunm in the input ExecRow.
	 * 
	 * @param rsNumber the result set number, not used
	 * @param colId the column number
	 */
	@Override
	void pushColumnReference(MethodBuilder mb, int rsNumber, int colId)
	{
		mb.getParameter(0);
		mb.push(colId);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Row, "getColumn", ClassName.DataValueDescriptor, 1);
	}

	private MethodBuilder beginRunExpressionMethod() {
		MethodBuilder mb = cb.newMethodBuilder(
				Modifier.PUBLIC,
				"void",
				"runExpression",
				new String[]{ClassName.ExecRow, ClassName.ExecRow});
		mb.addThrownException(ClassName.StandardException);
		return mb;
	}

	public void finishRunExpressionMethod(int indexColumnPosition) throws StandardException {
		// stack status when entering this function:
		// [size = 1]: value (top)
		executeMethod.getParameter(1);          // [size = 2]: value - indexRow (top)
		executeMethod.swap();                      // [size = 2]: indexRow - value (top)
		executeMethod.push(indexColumnPosition);   // [size = 3]: indexRow - value - position (top)
		executeMethod.swap();                      // [size = 3]: indexRow - position - value (top)
		executeMethod.upCast(ClassName.DataValueDescriptor);
		executeMethod.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Row, "setColumn", "void", 2);

		// stack should be empty now
		executeMethod.methodReturn();
		executeMethod.complete();

		// don't finish constructor too early since expression builder may still add code to it
		finishConstructor();
	}
}
