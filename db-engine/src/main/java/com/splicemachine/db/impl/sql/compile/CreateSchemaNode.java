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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * A CreateSchemaNode is the root of a QueryTree that 
 * represents a CREATE SCHEMA statement.
 *
 */

public class CreateSchemaNode extends DDLStatementNode
{
	private String 	name;
	private String	aid;
	
	/**
	 * Initializer for a CreateSchemaNode
	 *
	 * @param schemaName	The name of the new schema
	 * @param aid		 	The authorization id
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
			Object	schemaName,
			Object	aid)
		throws StandardException
	{
		/*
		** DDLStatementNode expects tables, null out
		** objectName explicitly to clarify that we
		** can't hang with schema.object specifiers.
		*/
		initAndCheck(null);	
	
		this.name = (String) schemaName;
		this.aid = (String) aid;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"schemaName: " + "\n" + name + "\n" +
				"authorizationId: " + "\n" + aid + "\n";
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind this createSchemaNode. Main work is to create a StatementPermission
	 * object to require CREATE_SCHEMA_PRIV at execution time.
	 */
	public void bindStatement() throws StandardException
	{
		CompilerContext cc = getCompilerContext();
		if (isPrivilegeCollectionRequired())
			cc.addRequiredSchemaPriv(name, aid, Authorizer.CREATE_SCHEMA_PRIV);

	}
	
	public String statementToString()
	{
		return "CREATE SCHEMA";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction()
	{
		return	getGenericConstantActionFactory().getCreateSchemaConstantAction(name, aid);
	}
}
