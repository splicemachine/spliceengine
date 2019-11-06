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

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.util.IdUtil;

/**
 * A TableName represents a qualified name, externally represented as a schema name
 * and an object name separated by a dot. This class is mis-named: it is used to
 * represent the names of other object types in addition to tables.
 *
 */

public class TableName extends QueryTreeNode
{
	/* Both schemaName and tableName can be null, however, if 
	** tableName is null then schemaName must also be null.
	*/
	String	tableName;
	String	schemaName;
	private boolean hasSchema;

	/**
	 * Initializer for when you have both the table and schema names.
	 *
	 * @param schemaName	The name of the schema being referenced
	 * @param tableName		The name of the table being referenced	 
	 */

	public void init(Object schemaName, Object tableName)
	{
		hasSchema = schemaName != null;
		this.schemaName = (String) schemaName;
		this.tableName = (String) tableName;
	}

	/**
	 * Initializer for when you have both the table and schema names.
	 *
	 * @param schemaName	The name of the schema being referenced
	 * @param tableName		The name of the table being referenced	 
	 * @param tokBeginOffset begin position of token for the table name 
	 *					identifier from parser.  pass in -1 if unknown
	 * @param tokEndOffset	end position of token for the table name 
	 *					identifier from parser.  pass in -1 if unknown
	 */
	public void init
	(
		Object schemaName, 
		Object tableName, 
		Object	tokBeginOffset,
		Object	tokEndOffset
	)
	{
		init(schemaName, tableName);
		this.setBeginOffset((Integer) tokBeginOffset);
		this.setEndOffset((Integer) tokEndOffset);
	}

	/**
	 * Get the table name (without the schema name).
	 *
	 * @return Table name as a String
	 */

	public String getTableName()
	{
		return tableName;
	}

	/**
	 * Return true if this instance was initialized with not null schemaName.
	 *
	 * @return true if this instance was initialized with not null schemaName
	 */
	
	public boolean hasSchema(){
		return hasSchema;
	}

	/**
	 * Get the schema name.
	 *
	 * @return Schema name as a String
	 */

	public String getSchemaName()
	{
		return schemaName;
	}

	/**
	 * Set the schema name.
	 *
	 * @param schemaName	 Schema name as a String
	 */

	public void setSchemaName(String schemaName)
	{
		this.schemaName = schemaName;
	}

	/**
	 * Get the full table name (with the schema name, if explicitly
	 * specified).
	 *
	 * @return Full table name as a String
	 */

	public String getFullTableName()
	{
		if (schemaName != null)
			return schemaName + "." + tableName;
		else
			return tableName;
	}

	/**
	* Get the full SQL name of this object, properly quoted and escaped.
	*/
	public  String  getFullSQLName()
	{
	    return IdUtil.mkQualifiedName( schemaName, tableName );
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (hasSchema)
			return getFullTableName();
		else
			return tableName;
	}

	/**
	 * 2 TableNames are equal if their both their schemaNames and tableNames are
	 * equal, or if this node's full table name is null (which happens when a
	 * SELECT * is expanded).  Also, only check table names if the schema
	 * name(s) are null.
	 *
	 * @param otherTableName	The other TableName.
	 *
	 * @return boolean		Whether or not the 2 TableNames are equal.
	 */
	public boolean equals(TableName otherTableName)
	{
        if( otherTableName == null)
            return false;
        
		String fullTableName = getFullTableName();
		if (fullTableName == null)
		{
			return true;
		}
		else if ((schemaName == null) || 
				 (otherTableName.getSchemaName() == null))
		{
			return tableName.equals(otherTableName.getTableName());
		}
		else
		{
			return fullTableName.equals(otherTableName.getFullTableName());
		}
	}

	/**
	 * 2 TableNames are equal if their both their schemaNames and tableNames are
	 * equal, or if this node's full table name is null (which happens when a
	 * SELECT * is expanded).  Also, only check table names if the schema
	 * name(s) are null.
	 *
	 * @param otherSchemaName	The other TableName.
	 * @param otherTableName	The other TableName.
	 *
	 * @return boolean		Whether or not the 2 TableNames are equal.
	 */
	public boolean equals(String otherSchemaName, String otherTableName)
	{
		String fullTableName = getFullTableName();
		if (fullTableName == null)
		{
			return true;
		}
		else if ((schemaName == null) || 
				 (otherSchemaName == null))
		{
			return tableName.equals(otherTableName);
		}
		else
		{
			return fullTableName.equals(otherSchemaName+"."+otherTableName);
		}
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	BIND METHODS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Bind this TableName. This means filling in the schema name if it
	  *	wasn't specified.
	  *
	  *	@param	dataDictionary	Data dictionary to bind against.
	  *
	  *	@exception StandardException		Thrown on error
	  */
	public void	bind( DataDictionary	dataDictionary )
		                       throws StandardException
	{
        schemaName = getSchemaDescriptor(schemaName).getSchemaName();
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	OBJECT INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Returns a hashcode for this tableName. This allows us to use TableNames
	  *	as keys in hash lists.
	  *
	  *	@return	hashcode for this tablename
	  */
	public	int	hashCode()
	{
		return	getFullTableName().hashCode();
	}

	/**
	  *	Compares two TableNames. Needed for hashing logic to work.
	  *
	  *	@param	other	other tableName
	  */
	public	boolean	equals( Object other )
	{
		if ( !( other instanceof TableName ) ) { return false; }

		TableName		that = (TableName) other;

		return	this.getFullTableName().equals( that.getFullTableName() );
	}

}
