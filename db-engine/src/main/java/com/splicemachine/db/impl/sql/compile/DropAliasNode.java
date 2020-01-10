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

import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

import com.splicemachine.db.catalog.AliasInfo;

/**
 * A DropAliasNode  represents a DROP ALIAS statement.
 *
 */

public class DropAliasNode extends DDLStatementNode
{
	private char aliasType;
	private char nameSpace;

	/**
	 * Initializer for a DropAliasNode
	 *
	 * @param dropAliasName	The name of the method alias being dropped
	 * @param aliasType				Alias type
	 *
	 * @exception StandardException
	 */
	public void init(Object dropAliasName, Object aliasType)
				throws StandardException
	{
		TableName dropItem = (TableName) dropAliasName;
		initAndCheck(dropItem);
		this.aliasType = (Character) aliasType;
	
		switch (this.aliasType)
		{
		    case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
			    nameSpace = AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR;
			    break;
			
	        case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("bad type to DropAliasNode: "+this.aliasType);
				}
		}
	}

	public	char	getAliasType() { return aliasType; }

	public String statementToString()
	{
		return "DROP " + aliasTypeName(aliasType);
	}

	/**
	 * Bind this DropMethodAliasNode.  
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		String			aliasName = getRelativeName();

		AliasDescriptor	ad = null;
		SchemaDescriptor sd = getSchemaDescriptor();
		
		if (sd.getUUID() != null) {
			ad = dataDictionary.getAliasDescriptor
			                          (sd.getUUID().toString(), aliasName, nameSpace );
		}
		if ( ad == null )
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, statementToString(), aliasName);
		}

		// User cannot drop a system alias
		if (ad.getSystemAlias())
		{
			throw StandardException.newException(SQLState.LANG_CANNOT_DROP_SYSTEM_ALIASES, aliasName);
		}

		// Statement is dependent on the AliasDescriptor
		getCompilerContext().createDependency(ad);
	}

	// inherit generate() method from DDLStatementNode


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropAliasConstantAction(getSchemaDescriptor(), getRelativeName(), nameSpace);
	}

	/* returns the alias type name given the alias char type */
	private static String aliasTypeName( char actualType)
	{
		String	typeName = null;

		switch ( actualType )
		{
		    case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
			    typeName = "DERBY AGGREGATE";
			    break;
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				typeName = "PROCEDURE";
				break;
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				typeName = "FUNCTION";
				break;
			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				typeName = "SYNONYM";
				break;
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				typeName = "TYPE";
				break;
		}
		return typeName;
	}
}
