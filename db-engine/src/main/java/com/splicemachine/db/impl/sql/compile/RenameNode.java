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

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.StatementType;

/**
 * A RenameNode is the root of a QueryTree that represents a
 * RENAME TABLE/COLUMN/INDEX statement.
 *
 */

public class RenameNode extends DDLStatementNode
{
	protected TableName newTableName;

	// original name of the object being renamed
	protected String oldObjectName;
	// original name for that object
	protected String newObjectName;

	protected TableDescriptor	td;
	private long conglomerateNumber;

	/* You can rename using either alter table or rename command to
	 * rename a table/column. An index can only be renamed with rename
	 * command. usedAlterTable flag is used to keep that information.
	 */
	protected boolean	usedAlterTable;

	/* renamingWhat will be set to 1 if user is renaming a table.
	 * Will be set to 2 if user is renaming a column and will be
	 * set to 3 if user is renaming an index
	 */
	protected int renamingWhat;

	/**
	 * Initializer for a RenameNode
	 *
	 * @param tableName The name of the table. This is the table which is
	 *		being renamed in case of rename table. In case of rename
	 *		column, the column being renamed belongs to this table.
	 *		In case of rename index, this is null because index name
	 *		is unique within a schema and doesn't have to be
	 *		associated with a table name
	 * @param oldObjectName This is either the name of column/index in case
	 *		of rename column/index. For rename table, this is null.
	 * @param newObjectName This is new name for table/column/index
	 * @param usedAlterTable True-Used Alter Table, False-Used Rename.
	 *		For rename index, this will always be false because
	 *		there is no alter table command to rename index
	 * @param renamingWhat Rename a 1 - table, 2 - column, 3 - index
	 *
	 * @exception StandardException Thrown on error
	 */
	public void init(Object tableName,
				   Object oldObjectName,
				   Object newObjectName,
				   Object usedAlterTable,
				   Object renamingWhat)
		throws StandardException
	{
		this.usedAlterTable = (Boolean) usedAlterTable;
		this.renamingWhat = (Integer) renamingWhat;

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				initAndCheck((TableName) tableName);
				this.newTableName =
					makeTableName(getObjectName().getSchemaName(),(String)newObjectName);
				this.oldObjectName = null;
				this.newObjectName = this.newTableName.getTableName();
				break;

			case StatementType.RENAME_COLUMN:
				/* coming from ALTER TABLE path, tableName will
				 * be TableName object. Coming from RENAME COLUMN
				 * path, tableName will be just a String.
				 */
				TableName actingObjectName;
				if (tableName instanceof TableName)
					actingObjectName = (TableName) tableName;
				else
					actingObjectName = makeTableName(null,
					(String)tableName);
				initAndCheck(actingObjectName);


				this.oldObjectName = (String)oldObjectName;
				this.newObjectName = (String)newObjectName;
				break;

			case StatementType.RENAME_INDEX:
				this.oldObjectName = (String)oldObjectName;
				this.newObjectName = (String)newObjectName;
				break;

			default:
				if (SanityManager.DEBUG) 
				SanityManager.THROWASSERT(
				"Unexpected rename action in RenameNode");
		}
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG) {

		switch (renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				return super.toString() +
				"oldTableName: " + "\n" + getRelativeName() + "\n" +
				"newTableName: " + "\n" + newTableName + "\n" ;

			case StatementType.RENAME_COLUMN:
				return super.toString() +
				"oldTableName.oldColumnName:" + "\n" +
				getRelativeName() + "." + oldObjectName + "\n" +
				"newColumnName: " + "\n" + newObjectName + "\n" ;

			case StatementType.RENAME_INDEX:
				return super.toString() +
				"oldIndexName:" + "\n" + oldObjectName + "\n" +
				"newIndexName: " + "\n" + newObjectName + "\n" ;

			default:
				SanityManager.THROWASSERT(
				"Unexpected rename action in RenameNode");
				return "UNKNOWN";
		}
		} else {
			return "";
		}
	}

	public String statementToString()
	{
		if (usedAlterTable)
			return "ALTER TABLE";
		else {
			switch (renamingWhat)
			{
				case StatementType.RENAME_TABLE:
					return "RENAME TABLE";

				case StatementType.RENAME_COLUMN:
					return "RENAME COLUMN";

				case StatementType.RENAME_INDEX:
					return "RENAME INDEX";

				default:
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT(
						"Unexpected rename action in RenameNode");
					return "UNKNOWN";
			}
    }
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this node.  This means doing any static error checking that
	 * can be done before actually renaming the table/column/index.
	 *
	 * For a table rename: looking up the from table, verifying it exists
	 * verifying it's not a system table, verifying it's not view
	 * and looking up to table, verifying it doesn't exist.
	 *
	 * For a column rename: looking up the table, verifying it exists,
	 * verifying it's not a system table, verifying it's not view, verifying
	 * the from column exists, verifying the to column doesn't exist.
	 *
	 * For a index rename: looking up the table, verifying it exists,
	 * verifying it's not a system table, verifying it's not view, verifying
	 * the from index exists, verifying the to index doesn't exist.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		DataDictionary			dd = getDataDictionary();
		ConglomerateDescriptor	cd;
		SchemaDescriptor sd;

		/* in case of rename index, the only thing we get from parser is
		 * current and new index names with no information about the
		 * table it belongs to. This is because index names are unique
		 * within a schema and hence then is no need to qualify an index
		 * name with a table name which we have to do for rename column.
		 * But from the index name, using the data dictionary, you can
		 * find the table it belongs to. Since most of the checking
		 * in bind is done using table descriptor, in the following if
		 * statement, we are trying to get the table information from the
		 * index name so it is available for the rest of he bind code.
		 */
		TableName baseTable;

		if (renamingWhat == StatementType.RENAME_INDEX) {

			sd = getSchemaDescriptor((String)null);

			ConglomerateDescriptor indexDescriptor =
				dd.getConglomerateDescriptor(oldObjectName, sd, false);
			if (indexDescriptor == null)
				throw StandardException.newException(
									 SQLState.LANG_INDEX_NOT_FOUND, oldObjectName);
			/* Get the table descriptor */
			td = dd.getTableDescriptor(indexDescriptor.getTableID());
			initAndCheck(makeTableName(td.getSchemaName(),
									   td.getName()));
		} else
			sd = getSchemaDescriptor();

		td = getTableDescriptor();

		//throw an exception if user is attempting a rename on temporary table
		if (td.isTemporary())
		{
			throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_TEMP_TABLE);
		}

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				/* Verify that new table name does not exist in the database */
				TableDescriptor td = getTableDescriptor(newObjectName, sd);
  				if (td != null)
					throw descriptorExistsException(td, sd);
				renameTableBind(dd);
				break;

			case StatementType.RENAME_COLUMN:
				renameColumnBind(dd);
				break;

			case StatementType.RENAME_INDEX:
				ConglomerateDescriptor conglomDesc = dd.getConglomerateDescriptor(newObjectName, sd, false);
  				if (conglomDesc != null)
					throw descriptorExistsException(conglomDesc, sd);
  				break;

			default:
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT(
							"Unexpected rename action in RenameNode");
				break;
		}

		conglomerateNumber = td.getHeapConglomerateId();

		/* Get the base conglomerate descriptor */
		cd = td.getConglomerateDescriptor(conglomerateNumber);

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	@Override
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If rename is on a SESSION schema table, then return true. 
		if (isSessionSchema(td.getSchemaName()))//existing table with rename action
			return true;

		//new name in rename action
		return renamingWhat == StatementType.RENAME_TABLE && isSessionSchema(getSchemaDescriptor());

	}

	/**
	 * Return true if the node references temporary tables no matter under which schema
	 *
	 * @return true if references temporary tables, else false
	 */
	@Override
	public boolean referencesTemporaryTable() {
		return td.isTemporary();
	}

	//do any checking needs to be done at bind time for rename table
	private void renameTableBind(DataDictionary dd)
		throws StandardException
	{
		/* Verify that there are no check constraints on the table */
		ConstraintDescriptorList constraintDescriptorList = dd.getConstraintDescriptors(td);
		int size =
			constraintDescriptorList == null ? 0 : constraintDescriptorList.size();

		ConstraintDescriptor constraintDescriptor;

		// go through all the constraints defined on the table
		for (int index = 0; index < size; index++)
		{
			constraintDescriptor = constraintDescriptorList.elementAt(index);
			// if it is a check constraint, error
			if (constraintDescriptor.getConstraintType() == DataDictionary.CHECK_CONSTRAINT)
			{
				throw StandardException.newException(
						SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
						"RENAME",
						td.getName(),
						"CONSTRAINT",
						constraintDescriptor.getConstraintName());
			}
		}
	}
                           
	//do any checking needs to be done at bind time for rename column
	private void renameColumnBind(DataDictionary dd)
		throws StandardException
	{
		ColumnDescriptor columnDescriptor = td.getColumnDescriptor(oldObjectName);

		/* Verify that old column name does exist in the table */
		if (columnDescriptor == null)
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
			oldObjectName, getFullName());

		/* Verify that new column name does not exist in the table */
		ColumnDescriptor cd = td.getColumnDescriptor(newObjectName);
  		if (cd != null)
  			throw descriptorExistsException(cd, td);

        //
		// You cannot rename a column which is referenced by the generation
        // clause of a generated column.
        //
        ColumnDescriptorList    generatedColumns = td.getGeneratedColumns();
        int                                 generatedColumnCount = generatedColumns.size();
        for ( int i = 0; i < generatedColumnCount; i++ )
        {
            ColumnDescriptor    gc = generatedColumns.elementAt( i );
            String[]                    referencedColumns = gc.getDefaultInfo().getReferencedColumnNames();
            int                         refColCount = referencedColumns.length;
            for (String refName : referencedColumns) {
                if (oldObjectName.equals(refName)) {
                    throw StandardException.newException(SQLState.LANG_GEN_COL_BAD_RENAME, oldObjectName, gc.getColumnName());
                }
            }
        }

		/* Verify that there are no check constraints using the column being renamed */
		ConstraintDescriptorList constraintDescriptorList =
			dd.getConstraintDescriptors(td);
		int size =
			constraintDescriptorList == null ? 0 : constraintDescriptorList.size();

		ConstraintDescriptor constraintDescriptor;
		ColumnDescriptorList checkConstraintCDL;
		int	checkConstraintCDLSize;

		// go through all the constraints defined on the table
		for (int index = 0; index < size; index++)
		{
			constraintDescriptor = constraintDescriptorList.elementAt(index);
			// if it is a check constraint, verify that column being
			// renamed is not used in it's sql
			if (constraintDescriptor.getConstraintType() == DataDictionary.CHECK_CONSTRAINT)
			{
				checkConstraintCDL = constraintDescriptor.getColumnDescriptors();
				checkConstraintCDLSize = checkConstraintCDL.size();

				for (int index2 = 0; index2 < checkConstraintCDLSize; index2++)
					if (checkConstraintCDL.elementAt( index2 ) == columnDescriptor)
						throw StandardException.newException(
						SQLState.LANG_RENAME_COLUMN_WILL_BREAK_CHECK_CONSTRAINT,
						oldObjectName,
						constraintDescriptor.getConstraintName());
			}
		}
	}
                           
	/**
	 * Create the Constant information that will drive the guts of Execution
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction()
		throws StandardException
	{
		return	getGenericConstantActionFactory().getRenameConstantAction(getFullName(),
			getRelativeName(),
			oldObjectName,
			newObjectName,
			getSchemaDescriptor(),
			td.getUUID(),
			usedAlterTable,
			renamingWhat);
	}

	private StandardException descriptorExistsException(TupleDescriptor tuple,
														TupleDescriptor parent)
	{
		return
			StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
										   tuple.getDescriptorType(),
										   tuple.getDescriptorName(),
										   parent.getDescriptorType(),
										   parent.getDescriptorName());
	}
}
