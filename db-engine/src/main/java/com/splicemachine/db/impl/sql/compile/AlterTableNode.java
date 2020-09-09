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
import com.splicemachine.db.iapi.reference.Limits;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.compile.Visitor;

import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.impl.sql.execute.ColumnInfo;

/**
 * A AlterTableNode represents a DDL statement that alters a table.
 * It contains the name of the object to be created.
 *
 */

public class AlterTableNode extends DDLStatementNode
{
	// The alter table action
	public	TableElementList	tableElementList = null;
	public  char				lockGranularity;

	/**
	 * updateStatistics will indicate that we are here for updating the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
	private	boolean				updateStatistics = false;
	/**
	 * The flag updateStatisticsAll will tell if we are going to update the 
	 * statistics of all indexes or just one index on a table. 
	 */
	private	boolean				updateStatisticsAll = false;
	/**
	 * dropStatistics will indicate that we are here for dropping the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
	private	    boolean					    dropStatistics;
	/**
	 * The flag dropStatisticsAll will tell if we are going to drop the 
	 * statistics of all indexes or just one index on a table. 
	 */
	private	    boolean					    dropStatisticsAll;
	/**
	 * If statistic is getting updated/dropped for just one index, then 
	 * indexNameForStatistics will tell the name of the specific index 
	 * whose statistics need to be updated/dropped.
	 */
	private	String				indexNameForStatistics;
	
	public	boolean				compressTable = false;
	public	boolean				sequential = false;
	//The following three (purge, defragment and truncateEndOfTable) apply for 
	//inplace compress
	public	boolean				purge = false;
	public	boolean				defragment = false;
	public	boolean				truncateEndOfTable = false;
	
	public	int					behavior;	// currently for drop column

	public	TableDescriptor		baseTable;

	private		int				changeType = UNKNOWN_TYPE;

	private boolean             truncateTable = false;

	// constant action arguments

	protected	SchemaDescriptor			schemaDescriptor = null;


	/**
	 * Initializer for a TRUNCATE TABLE
	 *
	 * @param objectName		The name of the table being truncated
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object objectName)
		throws StandardException
	{		
		initAndCheck(objectName);
		/* For now, this init() only called for truncate table */
		truncateTable = true;
		schemaDescriptor = getSchemaDescriptor();
	}
	
	/**
	 * Initializer for a AlterTableNode for COMPRESS using temporary tables
	 * rather than inplace compress
	 *
	 * @param objectName		The name of the table being altered
	 * @param sequential		Whether or not the COMPRESS is SEQUENTIAL
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object objectName,
					 Object sequential)
		throws StandardException
	{
		initAndCheck(objectName);

		this.sequential = (Boolean) sequential;
		/* For now, this init() only called for compress table */
		compressTable = true;

		schemaDescriptor = getSchemaDescriptor();
	}

	/**
	 * Initializer for a AlterTableNode for INPLACE COMPRESS
	 *
	 * @param objectName			The name of the table being altered
	 * @param purge					PURGE during INPLACE COMPRESS?
	 * @param defragment			DEFRAGMENT during INPLACE COMPRESS?
	 * @param truncateEndOfTable	TRUNCATE END during INPLACE COMPRESS?
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object objectName,
			 Object purge,
			 Object defragment,
			 Object truncateEndOfTable)
		throws StandardException
	{
		initAndCheck(objectName);

		this.purge = (Boolean) purge;
		this.defragment = (Boolean) defragment;
		this.truncateEndOfTable = (Boolean) truncateEndOfTable;
		compressTable = true;
		schemaDescriptor = getSchemaDescriptor(true, false);
	}

	/**
	 * Initializer for a AlterTableNode. The parameter values have different
	 *  meanings based on what kind of ALTER TABLE is taking place. 
	 *  
	 * @param objectName		The name of the table being altered
	 * @param changeType		ADD_TYPE or DROP_TYPE or UPDATE_STATISTICS or
	 *                          or DROP_STATISTICS
	 * @param param1 			For ADD_TYPE or DROP_TYPE, param1 gives the
	 *                          elements impacted by ALTER TABLE.
	 *                          For UPDATE_STATISTICS or or DROP_STATISTICS,
	 *                          param1 is boolean - true means update or drop
	 *                          the statistics of all the indexes on the table.
	 *                          False means, update or drop the statistics of
	 *                          only the index name provided by next parameter.
	 * @param param2 			For ADD_TYPE or DROP_TYPE, param2 gives the
	 *                          new lock granularity, if any
	 *                          For UPDATE_STATISTICS or DROP_STATISTICS,
	 *                          param2 can be the name of the specific index
	 *                          whose statistics will be dropped/updated. This
	 *                          param is used only if param1 is set to false
	 * @param param3			For DROP_TYPE, param3 can indicate if the drop
	 *                          column is CASCADE or RESTRICTED. This param is
	 *                          ignored for all the other changeType.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
							Object objectName,
							Object changeType,
							Object param1,
							Object param2,
							Object param3 )
		throws StandardException
	{
		initAndCheck(objectName);

		
		int[]	ct = (int[]) changeType;
		this.changeType = ct[0];
		
		switch ( this.changeType )
		{
		    case ADD_TYPE:
		    case DROP_TYPE:
		    case MODIFY_TYPE:
		    case LOCKING_TYPE:
				this.tableElementList = (TableElementList) param1;
				this.lockGranularity = (Character) param2;
				int[]	bh = (int[]) param3;
				this.behavior = bh[0];
				break;

		    case UPDATE_STATISTICS:
				this.updateStatisticsAll = (Boolean) param1;
				this.indexNameForStatistics = (String)param2;
				updateStatistics = true;
				break;

		    case DROP_STATISTICS:
				this.dropStatisticsAll = (Boolean) param1;
				this.indexNameForStatistics = (String)param2;
				dropStatistics = true;
				break;

		    default:

				throw StandardException.newException(SQLState.NOT_IMPLEMENTED);
		}

		schemaDescriptor = getSchemaDescriptor();
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
				"objectName: " + getObjectName() + "\n" +
				"lockGranularity: " + lockGranularity + "\n" +
				"compressTable: " + compressTable + "\n" +
				"sequential: " + sequential + "\n" +
				"truncateTable: " + truncateTable + "\n" +
				"purge: " + purge + "\n" +
				"defragment: " + defragment + "\n" +
				"truncateEndOfTable: " + truncateEndOfTable + "\n" +
				"updateStatistics: " + updateStatistics + "\n" +
				"updateStatisticsAll: " + updateStatisticsAll + "\n" +
				"dropStatistics: " + dropStatistics + "\n" +
				"dropStatisticsAll: " + dropStatisticsAll + "\n" +
				"indexNameForStatistics: " +
				indexNameForStatistics + "\n";
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
			if (tableElementList != null) {
				printLabel(depth, "tableElementList: ");
				tableElementList.treePrint(depth + 1);
			}

		}
	}

public String statementToString()
	{
		if(truncateTable)
			return "TRUNCATE TABLE";
		else
			return "ALTER TABLE";
	}

	public	int	getChangeType() { return changeType; }

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this AlterTableNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the user is not trying to add a 
	 * non-nullable column.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		DataDictionary	dd = getDataDictionary();
		int					numCheckConstraints = 0;
		int numReferenceConstraints = 0;
        int numGenerationClauses = 0;
		int numBackingIndexes = 0;

		/*
		** Get the table descriptor.  Checks the schema
		** and the table.
		*/
		if(compressTable && (purge || defragment || truncateEndOfTable)) {
			//We are dealing with inplace compress here and inplace compress is 
			//allowed on system schemas. In order to support inplace compress
			//on user as well as system tables, we need to use special 
			//getTableDescriptor(boolean) call to get TableDescriptor. This
			//getTableDescriptor(boolean) allows getting TableDescriptor for
			//system tables without throwing an exception.
			baseTable = getTableDescriptor(false);
		} else
			baseTable = getTableDescriptor();

		//throw an exception if user is attempting to alter a temporary table
		if (baseTable.isTemporary())
		{
			throw StandardException.newException(SQLState.LANG_NOT_ALLOWED_FOR_TEMP_TABLE);
		}

		/* Statement is dependent on the TableDescriptor */
		getCompilerContext().createDependency(baseTable);

		//If we are dealing with add column character type, then set that 
		//column's collation type to be the collation type of the schema.
		//The collation derivation of such a column would be "implicit".
		if (changeType == ADD_TYPE) {//the action is of type add.
			if (tableElementList != null) {//check if is is add column
				for (int i=0; i<tableElementList.size();i++) {
					if (tableElementList.elementAt(i) instanceof ColumnDefinitionNode) {
						ColumnDefinitionNode cdn = (ColumnDefinitionNode) tableElementList.elementAt(i);
						//check if we are dealing with add character column
                        //
                        // For generated columns which omit an explicit
                        // datatype, we have to defer this work until we bind
                        // the generation clause
                        //

                        if ( cdn.hasGenerationClause() && ( cdn.getType() == null ) ) { continue; }

                        if ( cdn.getType() == null )
                        {
                            throw StandardException.newException
                                ( SQLState.LANG_NEEDS_DATATYPE, cdn.getColumnName() );
                        }
                        
						if (cdn.getType().getTypeId().isStringTypeId()) {
							//we found what we are looking for. Set the 
							//collation type of this column to be the same as
							//schema descriptor's collation. Set the collation
							//derivation as implicit
							cdn.setCollationType(schemaDescriptor.getCollationType());
			        	}						
					}
				}
				
			}
		}
		if (tableElementList != null)
		{
			tableElementList.validate(this, dd, baseTable);

			/* Only 1012 columns allowed per table */
			if ((tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()) > Limits.DB2_MAX_COLUMNS_IN_TABLE)
			{
				throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
					String.valueOf(tableElementList.countNumberOfColumns() + baseTable.getNumberOfColumns()),
					getRelativeName(),
					String.valueOf(Limits.DB2_MAX_COLUMNS_IN_TABLE));
			}
			/* Number of backing indexes in the alter table statment */
			numBackingIndexes = tableElementList.countConstraints(DataDictionary.PRIMARYKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.FOREIGNKEY_CONSTRAINT) +
									tableElementList.countConstraints(DataDictionary.UNIQUE_CONSTRAINT);
			/* Check the validity of all check constraints */
			numCheckConstraints = tableElementList.countConstraints(
									DataDictionary.CHECK_CONSTRAINT);
            
            numReferenceConstraints = tableElementList.countConstraints(
									DataDictionary.FOREIGNKEY_CONSTRAINT);
            
            numGenerationClauses = tableElementList.countGenerationClauses();
		}

		//If the sum of backing indexes for constraints in alter table statement and total number of indexes on the table
		//so far is more than 32767, then we need to throw an exception 
		if ((numBackingIndexes + baseTable.getTotalNumberOfIndexes()) > Limits.DB2_MAX_INDEXES_ON_TABLE)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_INDEXES_ON_TABLE, 
				String.valueOf(numBackingIndexes + baseTable.getTotalNumberOfIndexes()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_INDEXES_ON_TABLE));
		}

		if ( (numCheckConstraints > 0) || (numGenerationClauses > 0) || (numReferenceConstraints > 0))
		{
			/* In order to check the validity of the check constraints and
			 * generation clauses
			 * we must goober up a FromList containing a single table, 
			 * the table being alter, with an RCL containing the existing and
			 * new columns and their types.  This will allow us to
			 * bind the constraint definition trees against that
			 * FromList.  When doing this, we verify that there are
			 * no nodes which can return non-deterministic results.
			 */
			FromList fromList = makeFromList( dd, tableElementList, false );
            FormatableBitSet    generatedColumns = baseTable.makeColumnMap( baseTable.getGeneratedColumns() );

			/* Now that we've finally goobered stuff up, bind and validate
			 * the check constraints and generation clauses.
			 */
			if  (numGenerationClauses > 0)
            { tableElementList.bindAndValidateGenerationClauses( schemaDescriptor, fromList, generatedColumns, baseTable ); }
			if  (numCheckConstraints > 0) { tableElementList.bindAndValidateCheckConstraints(fromList); }
            if ( numReferenceConstraints > 0) { tableElementList.validateForeignKeysOnGenerationClauses( fromList, generatedColumns ); }
		}

        // must be done after resolving the datatypes of the generation clauses
        if (tableElementList != null) { tableElementList.validatePrimaryKeyNullability(); }

		//Check if we are in alter table to update/drop the statistics. If yes,
		// then check if we are here to update/drop the statistics of a specific
		// index. If yes, then verify that the indexname provided is a valid one.
		if ((updateStatistics && !updateStatisticsAll) || (dropStatistics && !dropStatisticsAll))
		{
			ConglomerateDescriptor	cd = null;
			if (schemaDescriptor.getUUID() != null) 
				cd = dd.getConglomerateDescriptor(indexNameForStatistics, schemaDescriptor, false);

			if (cd == null)
			{
				throw StandardException.newException(
						SQLState.LANG_INDEX_NOT_FOUND, 
						schemaDescriptor.getSchemaName() + "." + indexNameForStatistics);
			}			
		}

		/* Unlike most other DDL, we will make this ALTER TABLE statement
		 * dependent on the table being altered.  In general, we try to
		 * avoid this for DDL, but we are already requiring the table to
		 * exist at bind time (not required for create index) and we don't
		 * want the column ids to change out from under us before
		 * execution.
		 */
		getCompilerContext().createDependency(baseTable);
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
		//If alter table is on a SESSION schema table, then return true. 
		return isSessionSchema(baseTable.getSchemaName());
	}

	/**
	 * Return true if the node references temporary tables no matter under which schema
	 *
	 * @return true if references temporary tables, else false
	 */
	@Override
	public boolean referencesTemporaryTable() {
		return baseTable.isTemporary();
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
        ColumnInfo[] colInfos = new ColumnInfo[0];
        int numConstraints = 0;

        if (tableElementList != null) {
            // Generate the ColumnInfo argument for the constant action. Keep the number of constraints.
            colInfos = new ColumnInfo[tableElementList.countNumberOfColumns()];

            numConstraints = tableElementList.genColumnInfos(colInfos,null);
        }

		/* If we've seen a constraint, then build a constraint list */
        ConstantAction[] conActions = new ConstantAction[0];
        if (numConstraints > 0) {
            conActions = getGenericConstantActionFactory().createConstraintConstantActionArray(numConstraints);

            tableElementList.genConstraintActions(false, conActions, getRelativeName(), schemaDescriptor,
                                                  getDataDictionary());

            for (ConstantAction cca : conActions) {
                if (getGenericConstantActionFactory().primaryKeyConstantActionCheck(cca)) {
                    DataDictionary dd = getDataDictionary();
                    // Check to see if a constraint of the same type
                    // already exists
                    ConstraintDescriptorList cdl =
                        dd.getConstraintDescriptors(baseTable);

                    if (cdl.getPrimaryKey() != null) {
                        throw StandardException.newException(
                            SQLState.LANG_ADD_PRIMARY_KEY_FAILED1,
                            baseTable.getQualifiedName());
                    }
                }
            }
        }

		return	getGenericConstantActionFactory().getAlterTableConstantAction(schemaDescriptor,
											 getRelativeName(),
											 baseTable.getUUID(),
											 baseTable.getHeapConglomerateId(),
											 TableDescriptor.BASE_TABLE_TYPE,
											 colInfos,
											 conActions,
											 lockGranularity,
											 compressTable,
											 behavior,
        								     sequential,
 										     truncateTable,
 										     purge,
 										     defragment,
 										     truncateEndOfTable,
 										     updateStatistics,
 										     updateStatisticsAll,
 										     dropStatistics,
 										     dropStatisticsAll,
 										     indexNameForStatistics);
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

}
