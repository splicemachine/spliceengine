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

import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * A CreateIndexNode is the root of a QueryTree that represents a CREATE INDEX
 * statement.
 *
 */

public class CreateIndexNode extends DDLStatementNode
{
	boolean				unique;
	boolean				uniqueWithDuplicateNulls;
	DataDictionary		dd = null;
	Properties			properties;
	String				indexType;
	TableName			indexName;
	TableName			tableName;
	Vector				columnNameList;
	String[]			columnNames = null;
	boolean[]			isAscending;
	int[]				boundColumnIDs;

	TableDescriptor		td;

	/**
	 * Initializer for a CreateIndexNode
	 *
	 * @param unique	True means it's a unique index
	 * @param indexType	The type of index
	 * @param indexName	The name of the index
	 * @param tableName	The name of the table the index will be on
	 * @param columnNameList	A list of column names, in the order they
	 *							appear in the index.
	 * @param properties	The optional properties list associated with the index.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
					Object unique,
					Object indexType,
					Object indexName,
					Object tableName,
					Object columnNameList,
					Object properties)
		throws StandardException
	{
		initAndCheck(indexName);
		this.unique = ((Boolean) unique).booleanValue();
		this.indexType = (String) indexType;
		this.indexName = (TableName) indexName;
		this.tableName = (TableName) tableName;
		this.columnNameList = (Vector) columnNameList;
		this.properties = (Properties) properties;
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
				"unique: " + unique + "\n" +
				"indexType: " + indexType + "\n" +
				"indexName: " + indexName + "\n" +
				"tableName: " + tableName + "\n" +
				"properties: " + properties + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CREATE INDEX";
	}


	public	boolean				getUniqueness() { return unique; }
	public	String				getIndexType() { return indexType; }
	public	TableName			getIndexName() { return indexName; }
	public	UUID				getBoundTableID() { return td.getUUID(); }
    public	Properties			getProperties() { return properties; }
	public  TableName			getIndexTableName() {return tableName; }
	public  String[]			getColumnNames() { return columnNames; }

	// get 1-based column ids
	public	int[]				getKeyColumnIDs() { return boundColumnIDs; }
	public	boolean[]			getIsAscending() { return isAscending; }

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateIndexNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the column name list does not
	 * contain any duplicate column names.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		SchemaDescriptor		sd;
		int						columnCount;

		sd = getSchemaDescriptor();

		td = getTableDescriptor(tableName);

		//If total number of indexes on the table so far is more than 32767, then we need to throw an exception
/*		if (td.getTotalNumberOfIndexes() > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE,
				String.valueOf(td.getTotalNumberOfIndexes()),
				tableName,
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}
*/
		/* Validate the column name list */
		verifyAndGetUniqueNames();

		columnCount = columnNames.length;
		boundColumnIDs = new int[ columnCount ];

		// Verify that the columns exist
		for (int i = 0; i < columnCount; i++)
		{
			ColumnDescriptor			columnDescriptor;

			columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			if (columnDescriptor == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
															columnNames[i],
															tableName);
			}
			boundColumnIDs[ i ] = columnDescriptor.getPosition();
			
			// set this only once -- if just one column does is missing "not null" constraint in schema
			uniqueWithDuplicateNulls = (! uniqueWithDuplicateNulls && (unique && ! columnDescriptor.hasNonNullDefault()));

			// Don't allow a column to be created on a non-orderable type
			if ( ! columnDescriptor.getType().getTypeId().
												orderable(getClassFactory()))
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
					columnDescriptor.getType().getTypeId().getSQLTypeName());
			}
		}

		/* Check for number of key columns to be less than 16 to match DB2 */
/*		if (columnCount > 16)
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEX_KEY_COLS);
*/
		/* See if the index already exists in this schema.
		 * NOTE: We still need to check at execution time
		 * since the index name is only unique to the schema,
		 * not the table.
		 */
//  		if (dd.getConglomerateDescriptor(indexName.getTableName(), sd, false) != null)
//  		{
//  			throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
//  												 "Index",
//  												 indexName.getTableName(),
//  												 "schema",
//  												 sd.getSchemaName());
//  		}

		/* Statement is dependent on the TableDescriptor */
		getCompilerContext().createDependency(td);

	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If create index is on a SESSION schema table, then return true.
		return isSessionSchema(td.getSchemaName());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		SchemaDescriptor		sd = getSchemaDescriptor();

		int columnCount = columnNames.length;
		int approxLength = 0;
		boolean index_has_long_column = false;


		// bump the page size for the index,
		// if the approximate sizes of the columns in the key are
		// greater than the bump threshold.
		// Ideally, we would want to have atleast 2 or 3 keys fit in one page
		// With fix for beetle 5728, indexes on long types is not allowed
		// so we do not have to consider key columns of long types
		for (int i = 0; i < columnCount; i++)
		{
			ColumnDescriptor columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			DataTypeDescriptor dts = columnDescriptor.getType();
			approxLength += dts.getTypeId().getApproximateLengthInBytes(dts);
		}


        if (approxLength > Property.IDX_PAGE_SIZE_BUMP_THRESHOLD)
        {

            if (((properties == null) ||
                 (properties.get(Property.PAGE_SIZE_PARAMETER) == null)) &&
                (PropertyUtil.getServiceProperty(
                     getLanguageConnectionContext().getTransactionCompile(),
                     Property.PAGE_SIZE_PARAMETER) == null))
            {
                // do not override the user's choice of page size, whether it
                // is set for the whole database or just set on this statement.

                if (properties == null)
                    properties = new Properties();

                properties.put(
                    Property.PAGE_SIZE_PARAMETER,
                    Property.PAGE_SIZE_DEFAULT_LONG);

            }
        }


		return getGenericConstantActionFactory().getCreateIndexConstantAction(
                    false, // not for CREATE TABLE
                    unique,
                    uniqueWithDuplicateNulls, // UniqueWithDuplicateNulls Index is a unique 
                    indexType,					//  index but with no "not null" constraint
                    sd.getSchemaName(),			//  on column in schema
                    indexName.getTableName(),
                    tableName.getTableName(),
                    td.getUUID(),
                    columnNames,
                    isAscending,
                    false,
                    null,
                    properties);
	}

	/**
	 * Check the uniqueness of the column names within the derived column list.
	 *
	 * @exception StandardException	Thrown if column list contains a
	 *											duplicate name.
	 */
	private void verifyAndGetUniqueNames()
				throws StandardException
	{
		int size = columnNameList.size();
		Hashtable	ht = new Hashtable(size + 2, (float) .999);
		columnNames = new String[size];
		isAscending = new boolean[size];

		for (int index = 0; index < size; index++)
		{
			/* Verify that this column's name is unique within the list
			 * Having a space at the end meaning descending on the column
			 */
			columnNames[index] = (String) columnNameList.get(index);
			if (columnNames[index].endsWith(" "))
			{
				columnNames[index] = columnNames[index].substring(0, columnNames[index].length() - 1);
				isAscending[index] = false;
			}
			else
				isAscending[index] = true;

			Object object = ht.put(columnNames[index], columnNames[index]);

			if (object != null &&
				((String) object).equals(columnNames[index]))
			{
				throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE_INDEX, columnNames[index]);
			}
		}
	}
}
