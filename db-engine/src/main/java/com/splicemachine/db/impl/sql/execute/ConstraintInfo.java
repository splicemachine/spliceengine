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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.dictionary.ConsInfo;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.ArrayUtil;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
 * This is a simple class used to store the run time information
 * about a constraint.
 *
 */
public class ConstraintInfo implements ConsInfo
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.  OR, since this is something that is used
	**	in stored prepared statements, it is ok to change it
	**	if you make sure that stored prepared statements are
	**	invalidated across releases.
	**
	********************************************************/

	/*
	** See the constructor for the meaning of these fields
	*/
	private String			tableName;
	private SchemaDescriptor	tableSd;
	private UUID				tableSchemaId;
	private String[]			columnNames;
	private int 				raDeleteRule;
	private int					raUpdateRule;


	/**
	 * Niladic constructor for Formattable
	 */
	public ConstraintInfo() {}

	/**
	 * Consructor
	 *
	 */
	public ConstraintInfo(
							String				tableName,
							SchemaDescriptor	tableSd,
							String[]			columnNames,
							int                 raDeleteRule,
							int                 raUpdateRule
						)
	{
		this.tableName = tableName;
		this.tableSd = tableSd;
		this.columnNames = columnNames;
		this.raDeleteRule  = raDeleteRule;
		this.raUpdateRule  = raUpdateRule;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(tableName);
		if (tableSd == null)
		{
			out.writeBoolean(false);
		}
		else
		{
			out.writeBoolean(true);
			out.writeObject(tableSd.getUUID());
		}

		if (columnNames == null)
		{
			out.writeBoolean(false);
		}
		else
		{
			out.writeBoolean(true);
			ArrayUtil.writeArrayLength(out, columnNames);
			ArrayUtil.writeArrayItems(out, columnNames);
		}

		//write referential actions for delete and update
		out.writeInt(raDeleteRule);
		out.writeInt(raUpdateRule);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		tableName = (String)in.readObject();
		if (in.readBoolean())
		{
			tableSchemaId = (UUID)in.readObject();
		}

		if (in.readBoolean())
		{
			columnNames = new String[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, columnNames);
		}

		//read referential actions for delete and update
		raDeleteRule = in.readInt();
		raUpdateRule = in.readInt();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.CONSTRAINT_INFO_V01_ID; }

	//////////////////////////////////////////////////////////////
	//
	// Misc
	//
	//////////////////////////////////////////////////////////////
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer str = new StringBuffer();
			str.append("Referencing ");
			str.append(tableName);
			if (columnNames != null)
			{
				str.append("(");
				for (int i = 0; i < columnNames.length; i++)
				{
					if (i > 0)
						str.append(",");

					str.append(columnNames[i]);
				}
				str.append(")");
			}	
		
			return str.toString();
		}
		else
		{
			return "";
		}
	}

	public SchemaDescriptor getReferencedTableSchemaDescriptor(DataDictionary dd)
		throws StandardException
	{
		if (tableSd != null)
		{
			return tableSd;
		}
		else
		{
			return dd.getSchemaDescriptor(tableSchemaId, null);
		}
	}
			
	public TableDescriptor getReferencedTableDescriptor(DataDictionary dd)
		throws StandardException
	{
		if (tableName == null)
		{
			return null;
		}
	
		return dd.getTableDescriptor(tableName, 
				getReferencedTableSchemaDescriptor(dd), null);
	}

	/**
	  *	This ConsInfo describes columns in a referenced table. What are
	  *	their names?
	  *
	  *	@return	array of referenced column names
	  */
	public String[] getReferencedColumnNames()
	{ return columnNames; }

	/**
	  *	Get the name of the table that these column live in.
	  *
	  *	@return	referenced table name
	  */
	public String getReferencedTableName()
	{ return tableName; }

	/**
	  *	Get the referential Action for an Update.
	  *
	  *	@return	referential Action for update
	  */
	public int getReferentialActionUpdateRule()
	{ return raUpdateRule; }

	
	/**
	  *	Get the referential Action for a Delete.
	  *
	  *	@return	referential Action Delete rule
	  */
	public int getReferentialActionDeleteRule()
	{ return raDeleteRule; }



}









