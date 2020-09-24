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

import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.depend.ProviderList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.util.Properties;

/**
 * A CreateTableNode is the root of a QueryTree that represents a CREATE TABLE or DECLARE GLOBAL TEMPORARY TABLE
 * statement.
 *
 * Note: If you change this implementation, BE SURE AND check the sqlgrammer.jj file
 * to make sure that special methods aren't being called!
 */

public class CreateTableNode extends DDLStatementNode
{
	private int                 createBehavior;
	private char				lockGranularity;
	private boolean				onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
	private boolean				onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
	private Properties			properties;
	private TableElementList	tableElementList;
	protected int	tableType; //persistent table or global temporary table
	private ResultColumnList	resultColumns;
	private ResultSetNode		queryExpression;
    private String              queryString;
	boolean             		  isExternal;
	ResultColumnList			  partitionedResultColumns;
	CharConstantNode              terminationChar;
	CharConstantNode              escapedByChar;
	CharConstantNode              linesTerminatedByChar;
	String              		  storageFormat;
	CharConstantNode              location;
	String              		  compression;
    boolean                       mergeSchema;
	boolean                       presplit;
	boolean                       isLogicalKey;
	String                        splitKeyPath;
	String                        columnDelimiter;
	String                        characterDelimiter;
	String                        timestampFormat;
	String                        dateFormat;
	String                        timeFormat;

	/**
	 * Initializer for a CreateTableNode for a base table
	 *
	 * @param newObjectName		The name of the new object being created (ie base table)
	 * @param tableElementList	The elements of the table: columns,
	 *				constraints, etc.
	 * @param properties		The optional list of properties associated with
	 *							the table.
	 * @param lockGranularity	The lock granularity.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
			Object newObjectName,
			Object createBehavior,
			Object tableElementList,
			Object properties,
			Object lockGranularity,
			Object isExternal,
			Object partitionedResultColumns,
			Object terminationChar,
			Object escapedByChar,
			Object linesTerminatedByChar,
			Object storageFormat,
			Object location,
			Object compression,
			Object mergeSchema) throws StandardException
	{
		this.createBehavior = ((Integer) createBehavior).intValue();
		this.isExternal = (Boolean) isExternal;
		if (this.isExternal)
			tableType = TableDescriptor.EXTERNAL_TYPE;
		else
			tableType = TableDescriptor.BASE_TABLE_TYPE;
		this.lockGranularity = (Character) lockGranularity;
		implicitCreateSchema = true;

		if (SanityManager.DEBUG)
		{
			if (this.lockGranularity != TableDescriptor.TABLE_LOCK_GRANULARITY &&
					this.lockGranularity != TableDescriptor.ROW_LOCK_GRANULARITY)
			{
				SanityManager.THROWASSERT(
						"Unexpected value for lockGranularity = " + this.lockGranularity);
			}
		}
		initAndCheck(newObjectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.properties = (Properties) properties;
		this.partitionedResultColumns = (ResultColumnList) partitionedResultColumns;
		this.terminationChar = (CharConstantNode) terminationChar;
		this.escapedByChar = (CharConstantNode) escapedByChar;
		this.linesTerminatedByChar = (CharConstantNode) linesTerminatedByChar;
		this.storageFormat = (String) storageFormat;
		this.location = (CharConstantNode) location;
		this.compression = (String) compression;
		this.mergeSchema = (Boolean)mergeSchema;
	}

	public void init(
			Object newObjectName,
			Object createBehavior,
			Object tableElementList,
			Object properties,
			Object lockGranularity,
			Object presplit,
			Object isLogicalKey,
			Object splitKeyPath,
			Object columnDelimiter,
			Object characterDelimiter,
			Object timestampFormat,
			Object dateFormat,
			Object timeFormat
		)
		throws StandardException
	{
		this.createBehavior = ((Integer) createBehavior).intValue();
		this.isExternal = (Boolean) isExternal;
		if (this.isExternal)
			tableType = TableDescriptor.EXTERNAL_TYPE;
		else
			tableType = TableDescriptor.BASE_TABLE_TYPE;
		this.lockGranularity = (Character) lockGranularity;
		implicitCreateSchema = true;

		if (SanityManager.DEBUG)
		{
			if (this.lockGranularity != TableDescriptor.TABLE_LOCK_GRANULARITY &&
				this.lockGranularity != TableDescriptor.ROW_LOCK_GRANULARITY)
			{
				SanityManager.THROWASSERT(
				"Unexpected value for lockGranularity = " + this.lockGranularity);
			}
		}
		initAndCheck(newObjectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.properties = (Properties) properties;
		this.presplit = (Boolean) presplit;
		this.isLogicalKey = (Boolean)isLogicalKey;
        this.splitKeyPath = splitKeyPath!=null ? ((CharConstantNode)splitKeyPath).getString() : null;
        this.columnDelimiter = columnDelimiter != null ? ((CharConstantNode)columnDelimiter).getString() : null;
        this.characterDelimiter = characterDelimiter != null ? ((CharConstantNode)characterDelimiter).getString() : null;
        this.timestampFormat = timestampFormat != null ? ((CharConstantNode)timestampFormat).getString() : null;
        this.dateFormat = dateFormat != null ? ((CharConstantNode)dateFormat).getString() : null;
        this.timeFormat = timeFormat != null ? ((CharConstantNode)timeFormat).getString() : null;
	}

	/**
	 * Initializer for a CreateTableNode for a global temporary table
	 *
	 * @param newObjectName		The name of the new object being declared (ie temporary table)
	 * @param tableElementList	The elements of the table: columns,
	 *				constraints, etc.
	 * @param properties		The optional list of properties associated with
	 *							the table.
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
			Object newObjectName,
			Object createBehavior,
			Object tableElementList,
			Object properties,
			Object onCommitDeleteRows,
			Object onRollbackDeleteRows)
		throws StandardException
	{
		this.createBehavior = ((Integer) createBehavior).intValue();
		tableType = TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE;
		lockGranularity = TableDescriptor.DEFAULT_LOCK_GRANULARITY;
		implicitCreateSchema = true;

		this.onCommitDeleteRows = (Boolean) onCommitDeleteRows;
		this.onRollbackDeleteRows = (Boolean) onRollbackDeleteRows;
		initAndCheck(newObjectName);
		this.tableElementList = (TableElementList) tableElementList;
		this.properties = (Properties) properties;

		if (SanityManager.DEBUG)
		{
			if (!this.onRollbackDeleteRows)
			{
				SanityManager.THROWASSERT(
				"Unexpected value for onRollbackDeleteRows = " + this.onRollbackDeleteRows);
			}
		}

	}
	
	/**
	 * Initializer for a CreateTableNode for a base table create from a query
	 * 
	 * @param newObjectName		The name of the new object being created
	 * 	                        (ie base table).
	 * @param resultColumns		The optional column list.
	 * @param queryExpression	The query expression for the table.
	 */
	public void init(
			Object newObjectName,
			Object createBehavior,
			Object resultColumns,
			Object queryExpression,
			Object isExternal,
			Object partitionedResultColumns,
			Object terminationChar,
			Object escapedByChar,
			Object linesTerminatedByChar,
			Object storageFormat,
			Object location)
		throws StandardException
	{
		this.createBehavior = ((Integer) createBehavior).intValue();
		tableType = TableDescriptor.BASE_TABLE_TYPE;
		lockGranularity = TableDescriptor.DEFAULT_LOCK_GRANULARITY;
		implicitCreateSchema = true;
		initAndCheck(newObjectName);
		this.resultColumns = (ResultColumnList) resultColumns;
		this.queryExpression = (ResultSetNode) queryExpression;
		this.isExternal = (Boolean) isExternal;
		this.partitionedResultColumns = (ResultColumnList) partitionedResultColumns;
		this.terminationChar = (CharConstantNode) terminationChar;
		this.escapedByChar = (CharConstantNode) escapedByChar;
		this.linesTerminatedByChar = (CharConstantNode) linesTerminatedByChar;
		this.storageFormat = (String) storageFormat;
		this.location = (CharConstantNode) location;
	}

    public void setQueryString(String queryString) {
        this.queryString = queryString;
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
			String tempString = "";
			if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			{
				tempString = tempString + "onCommitDeleteRows: " + "\n" + onCommitDeleteRows + "\n";
				tempString = tempString + "onRollbackDeleteRows: " + "\n" + onRollbackDeleteRows + "\n";
			} else
				tempString = tempString +
					(properties != null ?
					 "properties: " + "\n" + properties + "\n" :
					 "") +
					"lockGranularity: " + lockGranularity + "\n";
			return super.toString() +  tempString;
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 * @param depth		The depth to indent the sub-nodes
	 */
	public void printSubNodes(int depth) {
		if (SanityManager.DEBUG) {
			printLabel(depth, "tableElementList: ");
			tableElementList.treePrint(depth + 1);
		}
	}


	public String statementToString()
	{
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			return "DECLARE GLOBAL TEMPORARY TABLE";
		else
			return "CREATE TABLE";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateTableNode.  This means doing any static error checking that can be
	 * done before actually creating the base table or declaring the global temporary table.
	 * For eg, verifying that the TableElementList does not contain any duplicate column names.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		int numPrimaryKeys = 0;
		int numCheckConstraints = 0;
		int numReferenceConstraints = 0;
		int numUniqueConstraints = 0;
        int numGenerationClauses = 0;

        SchemaDescriptor sd = getSchemaDescriptor
            ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE, true);

        TableDescriptor td = getTableDescriptor( objectName.tableName, sd );
        if (td != null && createBehavior == StatementType.CREATE_IF_NOT_EXISTS)
        {
        	StandardException e = StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
					"table", objectName.tableName, "schema", sd.getSchemaName());
        	e.setSeverity(ExceptionSeverity.WARNING_SEVERITY);
        	throw e;
        }

		if (queryExpression != null)
		{
			FromList fromList = (FromList) getNodeFactory().getNode(
					C_NodeTypes.FROM_LIST,
					getNodeFactory().doJoinOrderOptimization(),
					getContextManager());
			
			CompilerContext cc = getCompilerContext();
			ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
			ProviderList apl = new ProviderList();
			
			try
			{
				cc.setCurrentAuxiliaryProviderList(apl);
				cc.pushCurrentPrivType(Authorizer.SELECT_PRIV);
				
				/* Bind the tables in the queryExpression */
				queryExpression =
					queryExpression.bindNonVTITables(dataDictionary, fromList);
				queryExpression = queryExpression.bindVTITables(fromList);
				
				/* Bind the expressions under the resultSet */
				queryExpression.bindExpressions(fromList);
				
				/* Bind the query expression */
				queryExpression.bindResultColumns(fromList);
				
				/* Reject any untyped nulls in the RCL */
				/* e.g. CREATE TABLE t1 (x) AS VALUES NULL WITH NO DATA */
				queryExpression.bindUntypedNullsToResultColumns(null);
			}
			finally
			{
				cc.popCurrentPrivType();
				cc.setCurrentAuxiliaryProviderList(prevAPL);
			}
			
			/* If there is an RCL for the table definition then copy the
			 * names to the queryExpression's RCL after verifying that
			 * they both have the same size.
			 */
			ResultColumnList qeRCL = queryExpression.getResultColumns();
			
			if (resultColumns != null)
			{
				if (resultColumns.size() != qeRCL.visibleSize())
				{
					throw StandardException.newException(
							SQLState.LANG_TABLE_DEFINITION_R_C_L_MISMATCH,
							getFullName());
				}
				qeRCL.copyResultColumnNames(resultColumns);
			}
			
			int schemaCollationType = sd.getCollationType();
	    
			/* Create table element list from columns in query expression */
			tableElementList = new TableElementList();
			
			for (int index = 0; index < qeRCL.size(); index++)
			{
				ResultColumn rc = qeRCL.elementAt(index);
				if (rc.isGenerated()) 
				{
					continue;
				}
				/* Raise error if column name is system generated. */
				if (rc.isNameGenerated())
				{
					throw StandardException.newException(
							SQLState.LANG_TABLE_REQUIRES_COLUMN_NAMES);
				}

				DataTypeDescriptor dtd = rc.getExpression().getTypeServices();
				if ((dtd != null) && !dtd.isUserCreatableType())
				{
					throw StandardException.newException(
							SQLState.LANG_INVALID_COLUMN_TYPE_CREATE_TABLE,
							dtd.getFullSQLTypeName(),
							rc.getName());
				}
				else if (dtd == null)
				{
					throw StandardException.newException(
							SQLState.LANG_INVALID_COLUMN_TYPE_CREATE_TABLE,
							"<UNKNOWN>",
							rc.getName());
				}
				//DERBY-2879  CREATE TABLE AS <subquery> does not maintain the 
				//collation for character types. 
				//eg for a territory based collation database
				//create table t as select tablename from sys.systables with no data;
				//Derby at this point does not support for a table's character 
				//columns to have a collation different from it's schema's
				//collation. Which means that in a territory based database, 
				//the query above will cause table t's character columns to
				//have collation of UCS_BASIC but the containing schema of t
				//has collation of territory based. This is not supported and
				//hence we will throw an exception below for the query above in
				//a territory based database. 
				if (dtd.getTypeId().isStringTypeId() &&
						dtd.getCollationType() != schemaCollationType)
				{
					throw StandardException.newException(
							SQLState.LANG_CAN_NOT_CREATE_TABLE,
							dtd.getCollationName(),
							DataTypeDescriptor.getCollationName(schemaCollationType));
				}

				ColumnDefinitionNode column = (ColumnDefinitionNode) getNodeFactory().getNode
                    ( C_NodeTypes.COLUMN_DEFINITION_NODE, rc.getName(), null, rc.getType(), null, getContextManager() );
				tableElementList.addTableElement(column);
			}
		} else {
			//Set the collation type and collation derivation of all the 
			//character type columns. Their collation type will be same as the 
			//collation of the schema they belong to. Their collation 
			//derivation will be "implicit". 
			//Earlier we did this in makeConstantAction but that is little too 
			//late (DERBY-2955)
			//eg 
			//CREATE TABLE STAFF9 (EMPNAME CHAR(20),
			//  CONSTRAINT STAFF9_EMPNAME CHECK (EMPNAME NOT LIKE 'T%'))
			//For the query above, when run in a territory based db, we need 
			//to have the correct collation set in bind phase of create table 
			//so that when LIKE is handled in LikeEscapeOperatorNode, we have 
			//the correct collation set for EMPNAME otherwise it will throw an 
			//exception for 'T%' having collation of territory based and 
			//EMPNAME having the default collation of UCS_BASIC
			tableElementList.setCollationTypesOnCharacterStringColumns(
				getSchemaDescriptor(
					tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
					true));
		}

		tableElementList.validate(this, dataDictionary, (TableDescriptor) null);

		/* Only 1012 columns allowed per table */
		if (tableElementList.countNumberOfColumns() > Limits.DB2_MAX_COLUMNS_IN_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(tableElementList.countNumberOfColumns()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_COLUMNS_IN_TABLE));
		}

		numPrimaryKeys = tableElementList.countConstraints(
								DataDictionary.PRIMARYKEY_CONSTRAINT);

		/* Only 1 primary key allowed per table */
		if (numPrimaryKeys > 1) {
			throw StandardException.newException(SQLState.LANG_TOO_MANY_PRIMARY_KEY_CONSTRAINTS, getRelativeName());
		}

		/* Check the validity of all check constraints */
		numCheckConstraints = tableElementList.countConstraints(
									DataDictionary.CHECK_CONSTRAINT);

		numReferenceConstraints = tableElementList.countConstraints(
									DataDictionary.FOREIGNKEY_CONSTRAINT);

		numUniqueConstraints = tableElementList.countConstraints(
									DataDictionary.UNIQUE_CONSTRAINT);

        numGenerationClauses = tableElementList.countGenerationClauses();

		if (tableType == TableDescriptor.EXTERNAL_TYPE) {
			if (numPrimaryKeys>0)
				throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_PRIMARY_KEYS,getRelativeName());
			if (numCheckConstraints>0)
				throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_CHECK_CONSTRAINTS,getRelativeName());
			if (numReferenceConstraints>0)
				throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_REFERENCE_CONSTRAINTS,getRelativeName());
			if (numUniqueConstraints>0)
				throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_UNIQUE_CONSTRAINTS,getRelativeName());
			if (numGenerationClauses>0)
				throw StandardException.newException(SQLState.EXTERNAL_TABLES_NO_GENERATION_CLAUSES,getRelativeName());

		}


		//temp tables can't have foreign key constraints defined on them
		if ((tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) &&
			(numReferenceConstraints > 0))
				throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_DECLARED_GLOBAL_TEMP_TABLE);

		//each of these constraints have a backing index in the back. We need to make sure that a table never has more
		//more than 32767 indexes on it and that is why this check.
		if ((numPrimaryKeys + numReferenceConstraints + numUniqueConstraints) > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numPrimaryKeys + numReferenceConstraints + numUniqueConstraints),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}

		if ( (numCheckConstraints > 0) || (numGenerationClauses > 0) || (numReferenceConstraints > 0) )
		{
			/* In order to check the validity of the check constraints and
			 * generation clauses
			 * we must goober up a FromList containing a single table,
			 * the table being created, with an RCL containing the
			 * new columns and their types.  This will allow us to
			 * bind the constraint definition trees against that
			 * FromList.  When doing this, we verify that there are
			 * no nodes which can return non-deterministic results.
			 */
			FromList fromList = makeFromList( null, tableElementList, true );
            FormatableBitSet    generatedColumns = new FormatableBitSet();

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints and generation clauses.
			 */
			if  (numGenerationClauses > 0) { tableElementList.bindAndValidateGenerationClauses( sd, fromList, generatedColumns, null ); }
			if  (numCheckConstraints > 0) { tableElementList.bindAndValidateCheckConstraints(fromList); }
            if ( numReferenceConstraints > 0) { tableElementList.validateForeignKeysOnGenerationClauses( fromList, generatedColumns ); }
		}

        if ( numPrimaryKeys > 0 ) {
			tableElementList.validatePrimaryKeyNullability();
		}
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
		//If table being created/declared is in SESSION schema, then return true.
		return isSessionSchema(
			getSchemaDescriptor(
				tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,
				true));
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		TableElementList		coldefs = tableElementList;

		// for each column, stuff system.column
		ColumnInfo[] colInfos = new ColumnInfo[coldefs.countNumberOfColumns()];

	    int numConstraints = coldefs.genColumnInfos(colInfos, this.partitionedResultColumns);

		/* If we've seen a constraint, then build a constraint list */
		ConstantAction[] conActions = null;

		SchemaDescriptor sd = getSchemaDescriptor(tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE,true);
		
		if (numConstraints > 0) {
			conActions = getGenericConstantActionFactory().createConstraintConstantActionArray(numConstraints);
			coldefs.genConstraintActions(true,conActions, getRelativeName(), sd, getDataDictionary());
		}

        // if the any of columns are "long" and user has not specified a
        // page size, set the pagesize to 32k.
        // Also in case where the approximate sum of the column sizes is
        // greater than the bump threshold , bump the pagesize to 32k

        boolean table_has_long_column = false; 
        int approxLength = 0;

        for (ColumnInfo colInfo : colInfos) {
            DataTypeDescriptor dts = colInfo.dataType;
            if (dts.getTypeId().isLongConcatableTypeId()) {
                table_has_long_column = true;
                break;
            }

            approxLength += dts.getTypeId().getApproximateLengthInBytes(dts);
        }

        if (table_has_long_column || (approxLength > Property.TBL_PAGE_SIZE_BUMP_THRESHOLD))
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

		return(
            getGenericConstantActionFactory().getCreateTableConstantAction(
                    sd.getSchemaName(),
                    getRelativeName(),
                    tableType,
                    colInfos,
                    conActions,
                    properties,
                    createBehavior,
                    lockGranularity,
                    onCommitDeleteRows,
                    onRollbackDeleteRows,
                    queryString,
                    isExternal,
                    terminationChar!=null?terminationChar.value.getString():null,
                    escapedByChar!=null?escapedByChar.value.getString():null,
                    linesTerminatedByChar!=null?linesTerminatedByChar.value.getString():null,
                    storageFormat,
                    location!=null?location.value.getString():null,
                    compression,
                    mergeSchema,
                    presplit,
                    isLogicalKey,
                    splitKeyPath,
                    columnDelimiter,
                    characterDelimiter,
                    timestampFormat,
                    dateFormat,
                    timeFormat
            ));
    }

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);

		if (tableElementList != null)
		{
			tableElementList.accept(v, this);
		}
	}

	@Override
	public void optimizeStatement() throws StandardException {
    	for(int i=tableElementList.size()-1; i>=0; --i) {
    		Object element = tableElementList.elementAt(i);
			if(element instanceof ConstraintDefinitionNode) {
				if(((ConstraintDefinitionNode)element).canBeIgnored()) {
					tableElementList.removeElementAt(i);
				}
			}
		}
	}
}
