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

package com.splicemachine.db.iapi.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

/**
 * The Statement interface provides a way of giving a statement to the
 * language module, preparing the statement, and executing it. It also
 * provides some support for stored statements. Simple, non-stored,
 * non-parameterized statements can be executed with the execute() method.
 * Parameterized statements must use prepare(). To get the stored query
 * plan for a statement, use get().
 * <p>
 * This interface will have different implementations for the execution-only
 * and compile-and-execute versions of the product. In the execution-only
 * version, some of the methods will do nothing but raise exceptions to
 * indicate that they are not implemented.
 * <p>
 * There is a Statement factory in the Connection interface in the Database
 * module, which uses the one provided in LanguageFactory.
 *
 */
public interface Statement
{

	/**
	 * Generates an execution plan without executing it.
	 *
	 * @return A PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	PreparedStatement	prepare(LanguageConnectionContext lcc) throws StandardException;
	/**
	 * Generates an execution plan without executing it.
	 *
	 * @param 	lcc			the language connection context
	 * @param 	allowInternalSyntax	If this statement is for a metadata call then 
	 *	   we will allow internal sql syntax on such statement. This internal
	 *	   sql syntax is not available to a user sql statement.
	 *
	 * @return A PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	PreparedStatement	prepare(LanguageConnectionContext lcc, boolean allowInternalSyntax) throws StandardException;
	
	/**
	 * Generates an execution plan given a set of named parameters.
	 * For generating a storable prepared statement (which
	 * has some extensions over a standard prepared statement).
	 *
	 * @param 	lcc					Compiler state variable.
	 * @param 	ps					Prepared statement
	 * @param	paramDefaults		Default parameter values to use for
	 *								optimization
	 * @param	spsSchema schema of the stored prepared statement
	 *
	 * @return A Storable PreparedStatement that allows execution of the execution
	 *	   plan.
	 * @exception StandardException	Thrown if this is an
	 *	   execution-only version of the module (the prepare() method
	 *	   relies on compilation).
	 */
	PreparedStatement	prepareStorable
	(
			LanguageConnectionContext lcc,
			PreparedStatement ps,
			Object[] paramDefaults,
			SchemaDescriptor spsSchema,
			boolean internalSQL
	)
		throws StandardException;

	/**
	 *	Return the SQL string that this statement is for.
	 *
	 *	@return the SQL string this statement is for.
	 */
	String getSource();


	/**
	 * Return the String of session property values set when the plan is compiled
	 * @return the String of session property values set when the plan is compiled
	 */
	String getSessionPropertyValues();
}
