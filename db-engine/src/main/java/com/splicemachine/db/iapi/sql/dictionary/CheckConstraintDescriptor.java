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

package com.splicemachine.db.iapi.sql.dictionary;
import com.splicemachine.db.catalog.ReferencedColumns;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;

/**
 * This class represents a check constraint descriptor.
 *
 */
public class CheckConstraintDescriptor extends ConstraintDescriptor
{
	private ReferencedColumns	referencedColumns;
	private String						constraintText;

	CheckConstraintDescriptor(
		    DataDictionary dataDictionary,
			TableDescriptor table,
			String constraintName,
			boolean deferrable,
			boolean initiallyDeferred,
			UUID constraintId,
			String constraintText,
			ReferencedColumns referencedColumns,
			SchemaDescriptor schemaDesc,
			boolean	isEnabled
			)							
	{
		super(dataDictionary, table, constraintName, deferrable,
			  initiallyDeferred, (int []) null,
			  constraintId, schemaDesc, isEnabled);
		this.constraintText = constraintText;
		this.referencedColumns = referencedColumns;
	}

	/**
	 * Does this constraint have a backing index?
	 *
	 * @return boolean	Whether or not there is a backing index for this constraint.
	 */
	public boolean hasBackingIndex()
	{
		return false;
	}

	/**
	 * Gets an identifier telling what type of descriptor it is
	 * (UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 *
	 * @return	An identifier telling what type of descriptor it is
	 *		(UNIQUE, PRIMARY KEY, FOREIGN KEY, CHECK).
	 */
	public int	getConstraintType()
	{
		return DataDictionary.CHECK_CONSTRAINT;
	}

	/**
	 * Get the text of the constraint. (Only non-null/meaningful for check
	 * constraints.)
	 * @return	The constraint text.
	 */
	public String getConstraintText()
	{
		return constraintText;
	}

	/**
	 * Get the UUID of the backing index, if one exists.
	 *
	 * @return The UUID of the backing index, if one exists, else null.
	 */
	public UUID getConglomerateId()
	{
		return null;
	}

	/**
	 * Get the ReferencedColumns.
	 *
	 * @return The ReferencedColumns.
	 */
	public ReferencedColumns getReferencedColumnsDescriptor()
	{
		return referencedColumns;
	}

	/**
	 * Set the ReferencedColumns; used in drop column
	 *
	 * @param	rcd	The new ReferencedColumns.
	 */
	public void setReferencedColumnsDescriptor(ReferencedColumns rcd)
	{
		referencedColumns = rcd;
	}

	/**
	 * Get the referenced columns as an int[] of column ids.
	 *
	 * @return The array of referenced column ids.
	 */
	public int[] getReferencedColumns()
	{
		return referencedColumns.getReferencedColumnPositions();
	}

	/**
	 * Does this constraint need to fire on this type of
	 * DML?  For a check constraint, all inserts, and
	 * appropriate updates
	 *
	 * @param stmtType	the type of DML 
	 * (StatementType.INSERT|StatementType.UPDATE|StatementType.DELETE)
	 * @param modifiedCols	the columns modified, or null for all
	 *
	 * @return true/false
	 */
	public boolean needsToFire(int stmtType, int[] modifiedCols)
	{
		/*
		** If we are disabled, we never fire
		*/
		if (!isEnabled)
		{
			return false;
		}

		if (stmtType == StatementType.INSERT)
		{
			return true;
		}

		if (stmtType == StatementType.DELETE)
		{
			return false;
		}
	
		// if update, only relevant if columns intersect
		return doColumnsIntersect(modifiedCols, getReferencedColumns());
	}   

	/**
	 * Convert the CheckConstraintDescriptor to a String.
	 *
	 * @return	A String representation of this CheckConstraintDescriptor
	 */

	public String	toString()
	{
		if (SanityManager.DEBUG)
		{
			return "constraintText: " + constraintText + "\n" +
			   "referencedColumns: " + referencedColumns + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}


}
