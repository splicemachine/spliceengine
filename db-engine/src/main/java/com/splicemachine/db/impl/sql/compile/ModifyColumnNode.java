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

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DefaultDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.KeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;

import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;

/**
 * A ModifyColumnNode represents a modify column in an ALTER TABLE statement.
 *
 */

public class ModifyColumnNode extends ColumnDefinitionNode
{
	int		columnPosition = -1;
	UUID	oldDefaultUUID;

	/**
	 * Get the UUID of the old column default.
	 *
	 * @return The UUID of the old column default.
	 */
	UUID getOldDefaultUUID()
	{
		return oldDefaultUUID;
	}

	/**
	 * Get the column position for the column.
	 *
	 * @return The column position for the column.
	 */
	public int getColumnPosition()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(columnPosition > 0,
				"columnPosition expected to be > 0");
		}
		return columnPosition;
	}

	/**
	 * Check the validity of a user type.  Checks that
	 * 1. the column type is either varchar, ....
	 * 2. is the same type after the alter.
	 * 3. length is greater than the old length.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void checkUserType(TableDescriptor td)
		throws StandardException
	{
		if (getNodeType() != C_NodeTypes.MODIFY_COLUMN_TYPE_NODE)
			return;				// nothing to do if user not changing length

        ColumnDescriptor cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(
				SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}
		
		DataTypeDescriptor oldType = cd.getType();
        setNullability(oldType.isNullable());

		// can't change types yet.
		if (!(oldType.getTypeId().equals(getType().getTypeId())))
		{
			throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_CHANGE_TYPE, name);
		}			
		
		// can only alter the length of varchar, bitvarying columns
		String typeName = getType().getTypeName();
		if (!(typeName.equals(TypeId.VARCHAR_NAME)) &&
			!(typeName.equals(TypeId.VARBIT_NAME))&&
			!(typeName.equals(TypeId.BLOB_NAME))&&
			!(typeName.equals(TypeId.CLOB_NAME)))
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_TYPE);
		}
		
		// cannot decrease the length of a column
		if (getType().getMaximumWidth() < oldType.getMaximumWidth())
		{
			throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_INVALID_LENGTH, name);
		}
	}
	
	/**
     * Check if the the column can be modified, and throw error if not.
     *
	 * If the type of a column is being changed (for instance if the length 
     * of the column is being increased) then make sure that this does not 
     * violate any key constraints; 
	 * the column being altered is 
	 *   1. part of foreign key constraint 
	 *         ==> ERROR. This references a Primary Key constraint and the
	 *             type & lengths of the pkey/fkey must match exactly.
	 *   2. part of a unique/primary key constraint
	 *         ==> OK if no fkey references this constraint.
	 *         ==> ERROR if any fkey in the system references this constraint.
	 *
	 * @param td		The Table Descriptor on which the ALTER is being done.
	 *
	 * @exception StandardException		Thrown on Error.
	 *
	 */
	public void checkExistingConstraints(TableDescriptor td)
	             throws StandardException
	{
		if ((getNodeType() != C_NodeTypes.MODIFY_COLUMN_TYPE_NODE) &&
			(getNodeType() != C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE) &&
			(getNodeType() != C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE))
			return;

		DataDictionary           dd          = getDataDictionary();
		ConstraintDescriptorList cdl         = dd.getConstraintDescriptors(td);
		int                      intArray[]  = new int[1];
		intArray[0]                          = columnPosition;

		for (int index = 0; index < cdl.size(); index++)
		{
			ConstraintDescriptor existingConstraint =
				                                cdl.elementAt(index);

			if (!(existingConstraint instanceof KeyConstraintDescriptor))
				continue;

			if (!existingConstraint.columnIntersects(intArray))
				continue;
															 
			int constraintType = existingConstraint.getConstraintType();

			// cannot change the length of a column that is part of a 
			// foreign key constraint. Must be an exact match between pkey
			// and fkey columns.
			if ((constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT) 
				&&
				(getNodeType() == C_NodeTypes.MODIFY_COLUMN_TYPE_NODE))
			{
				throw StandardException.newException(
					 SQLState.LANG_MODIFY_COLUMN_FKEY_CONSTRAINT, 
                     name, existingConstraint.getConstraintName());
			}	
			else
			{
				// a column that is part of a primary key
                // is being made nullable; can't be done.
				if ((getNodeType() == 
					 C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE) &&
					((existingConstraint.getConstraintType() == 
					 DataDictionary.PRIMARYKEY_CONSTRAINT)))
				{
                    throw StandardException.newException(SQLState.LANG_MODIFY_COLUMN_EXISTING_PRIMARY_KEY, name);
				}
				// unique key or primary key.
				ConstraintDescriptorList 
					refcdl = dd.getForeignKeys(existingConstraint.getUUID());
				 
				if (refcdl.size() > 0)
				{
					throw StandardException.newException(
						 SQLState.LANG_MODIFY_COLUMN_REFERENCED, 
                         name, refcdl.elementAt(0).getConstraintName());
				}
				
				// Make the statement dependent on the primary key constraint.
				getCompilerContext().createDependency(existingConstraint);
			}
		}
    }

	/**
	 * If the column being modified is of character string type, then it should
	 * get its collation from the corresponding column in the TableDescriptor.
	 * This will ensure that at alter table time, the existing character string
	 * type columns do not loose their collation type. If the alter table is 
	 * doing a drop column, then we do not need to worry about collation info.
	 * 
	 * @param td Table Descriptor that holds the column which is being altered
	 * @throws StandardException
	 */
	public void useExistingCollation(TableDescriptor td)
    throws StandardException
    {
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}
		//getType() == null means we are dealing with drop column and hence 
		//no need to worry about collation info
		if (getType() != null) {
			if (getType().getTypeId().isStringTypeId()) {
				setCollationType(cd.getType().getCollationType());			
			}
		}
    }
	/**
	 * Get the action associated with this node.
	 *
	 * @return The action associated with this node.
	 */
	int getAction()
	{
		switch (getNodeType())
		{
		case C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
			if (autoinc_create_or_modify_Start_Increment == ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE)
				return ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART;
			else if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE)
				return ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT;
			else
				return ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE;
		case C_NodeTypes.MODIFY_COLUMN_TYPE_NODE:
			return ColumnInfo.MODIFY_COLUMN_TYPE;
		case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
			return ColumnInfo.MODIFY_COLUMN_CONSTRAINT;
		case C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
			return ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL;
		case C_NodeTypes.DROP_COLUMN_NODE:
			return ColumnInfo.DROP;
		default:
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected nodeType = " + 
										  getNodeType());
			}
			return 0;
		}
	}

	/**
	 * Check the validity of the default, if any, for this node.
	 *
	 * @param dd		The DataDictionary.
	 * @param td		The TableDescriptor.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateDefault(DataDictionary dd, TableDescriptor td) 
		throws StandardException
	{
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}


		// Get the UUID for the old default
		DefaultDescriptor defaultDescriptor = cd.getDefaultDescriptor(dd);
		
		oldDefaultUUID = (defaultDescriptor == null) ? null : defaultDescriptor.getUUID();

		// Remember the column position
		columnPosition = cd.getPosition();

		// No other work to do if no user specified default
		if (getNodeType() != C_NodeTypes.MODIFY_COLUMN_DEFAULT_NODE)
		{
			return;
		}

		// If the statement is not setting the column's default, then
		// recover the old default and re-use it. If the statement is
		// changing the start value for the auto-increment, then recover
		// the old increment-by value and re-use it. If the statement is
		// changing the increment-by value, then recover the old start value
		// and re-use it. This way, the column alteration only changes the
		// aspects of the autoincrement settings that it intends to change,
		// and does not lose the other aspecs.
		if (keepCurrentDefault)
        { defaultInfo = (DefaultInfoImpl)cd.getDefaultInfo(); }
        else
        {
            if ( cd.hasGenerationClause() )
            {
				throw StandardException.newException( SQLState.LANG_GEN_COL_DEFAULT, cd.getColumnName() );
            }
        }
		if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE)
			autoincrementIncrement = cd.getAutoincInc();
		if (autoinc_create_or_modify_Start_Increment ==
				ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE)
			autoincrementStart = cd.getAutoincStart();

		/* Fill in the DataTypeServices from the DataDictionary */
		type = cd.getType();

		// Now validate the default
		validateDefault(dd, td);
	}
	
	private ColumnDescriptor getLocalColumnDescriptor(String name, TableDescriptor td)
	         throws StandardException
	{
		ColumnDescriptor cd;

		// First verify that the column exists
		cd = td.getColumnDescriptor(name);
		if (cd == null)
		{
			throw StandardException.newException(
				SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, name, td.getName());
		}

		return cd;
	}
	/**
	 * check the validity of autoincrement values in the case that we are 
	 * modifying an existing column (includes checking if autoincrement is set
	 * when making a column nullable)
	 */
	public void validateAutoincrement(DataDictionary dd, TableDescriptor td, int tableType)
	         throws StandardException
	{
		ColumnDescriptor cd;

		// a column that has an autoincrement default can't be made nullable
		if (getNodeType() == C_NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE)
		{
			cd = getLocalColumnDescriptor(name, td);
			if (cd.isAutoincrement())
			{
				throw StandardException.newException(SQLState.LANG_AI_CANNOT_NULL_AI,
						getColumnName());
			}
		}

		if (autoincrementVerify)
		{
			cd = getLocalColumnDescriptor(name, td);
			if (!cd.isAutoincrement())
				throw StandardException.newException(SQLState.LANG_INVALID_ALTER_TABLE_ATTRIBUTES,
								td.getQualifiedName(), name);
		}
		if (isAutoincrement == false)
			return;
		
		super.validateAutoincrement(dd, td, tableType);
		if (getType().isNullable())
			throw StandardException.newException(SQLState.LANG_AI_CANNOT_ADD_AI_TO_NULLABLE,
												getColumnName());
	}
}
