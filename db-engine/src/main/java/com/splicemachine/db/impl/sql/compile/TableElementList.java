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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.depend.ProviderList;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ConstraintInfo;
import splice.com.google.common.collect.Lists;

import java.util.*;

/**
 * A TableElementList represents the list of columns and other table elements
 * such as constraints in a CREATE TABLE or ALTER TABLE statement.
 *
 */

public class TableElementList extends QueryTreeNodeVector {
	private int				numColumns;
	private TableDescriptor td;

	/**
	 * Add a TableElementNode to this TableElementList
	 *
	 * @param tableElement	The TableElementNode to add to this list
	 */

	public void addTableElement(TableElementNode tableElement)
	{
		addElement(tableElement);
		if ((tableElement instanceof ColumnDefinitionNode) ||
			tableElement.getElementType() == TableElementNode.AT_DROP_COLUMN)
		{
			numColumns++;
		}
	} 

	/**
	 * Use the passed schema descriptor's collation type to set the collation
	 * of the character string types in create table node
	 * @param sd
	 */
	void setCollationTypesOnCharacterStringColumns(SchemaDescriptor sd)
        throws StandardException
    {
		int			size = size();
		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (tableElement instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) elementAt(index);

                setCollationTypeOnCharacterStringColumn( sd, cdn );
			}
		}
	}

	/**
	 * Use the passed schema descriptor's collation type to set the collation
	 * of a character string column.
	 * @param sd
	 */
	void setCollationTypeOnCharacterStringColumn(SchemaDescriptor sd, ColumnDefinitionNode cdn )
        throws StandardException
    {
		int collationType = sd.getCollationType();

        //
        // Only generated columns can omit the datatype specification during the
        // early phases of binding--before we have been able to bind the
        // generation clause.
        //
        DataTypeDescriptor  dtd = cdn.getType();
        if ( dtd == null )
        {
            if ( cdn.hasGenerationClause() )
            {
            }
            else
            {
                throw StandardException.newException
                    ( SQLState.LANG_NEEDS_DATATYPE, cdn.getColumnName() );
            }
        }
        else
        {
            if ( dtd.getTypeId().isStringTypeId() ) { cdn.setCollationType(collationType); }
        }
    }
    
	/**
	 * Validate this TableElementList.  This includes checking for
	 * duplicate columns names, and checking that user types really exist.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 * @param dd		DataDictionary to use
	 * @param td		TableDescriptor for table, if existing table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void validate(DDLStatementNode ddlStmt,
					     DataDictionary dd,
						 TableDescriptor td)
					throws StandardException
	{
 		this.td = td;
		int numAutoCols = 0;

		int			size = size();
		Hashtable	columnHT = new Hashtable(size + 2, (float) .999);
		Hashtable	constraintHT = new Hashtable(size + 2, (float) .999);
		//all the primary key/unique key constraints for this table
		Vector constraintsVector = new Vector();

		//special case for alter table (td is not null in case of alter table)
		if (td != null)
		{
			//In case of alter table, get the already existing primary key and unique
			//key constraints for this table. And then we will compare them with  new
			//primary key/unique key constraint column lists.
			ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
			if (cdl != null) //table does have some pre-existing constraints defined on it
			{
				ConstraintDescriptor cd;
				for (int i=0; i<cdl.size();i++)
				{
					cd = cdl.elementAt(i);
					//if the constraint type is not primary key or unique key, ignore it.
					if (cd.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT ||
					cd.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT)
						constraintsVector.addElement(cd);
				}
			}
		}

		int tableType = TableDescriptor.BASE_TABLE_TYPE;
		if (ddlStmt instanceof CreateTableNode)
			tableType = ((CreateTableNode)ddlStmt).tableType;

		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (tableElement instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) elementAt(index);
				if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE &&
					(cdn.getType().getTypeId().isLongConcatableTypeId() ||
					cdn.getType().getTypeId().isUserDefinedTypeId()))
				{
					throw StandardException.newException(SQLState.LANG_LONG_DATA_TYPE_NOT_ALLOWED, cdn.getColumnName());
				}
				checkForDuplicateColumns(ddlStmt, columnHT, cdn.getColumnName());
				cdn.checkUserType(td);
				cdn.bindAndValidateDefault(dd, td);

				cdn.validateAutoincrement(dd, td, tableType);

				if (tableElement instanceof ModifyColumnNode)
				{
					ModifyColumnNode mcdn = (ModifyColumnNode)cdn;
					mcdn.checkExistingConstraints(td);
					mcdn.useExistingCollation(td);

				} else if (cdn.isAutoincrementColumn())
                { numAutoCols ++; }
			}
			else if (tableElement.getElementType() == TableElementNode.AT_DROP_COLUMN)
			{
				String colName = tableElement.getName();
				if (td.getColumnDescriptor(colName) == null)
				{
					throw StandardException.newException(
												SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
												colName,
												td.getQualifiedName());
				}
				break;
			}

			/* The rest of this method deals with validating constraints */
			if (! (tableElement.hasConstraint()))
			{
				continue;
			}

			ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) tableElement;

			cdn.bind(ddlStmt, dd);

			//if constraint is primary key or unique key, add it to the vector
			if (cdn.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT ||
			cdn.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT)
			{
				/* In case of create table, the vector can have only ConstraintDefinitionNode
				* elements. In case of alter table, it can have both ConstraintDefinitionNode
				* (for new constraints) and ConstraintDescriptor(for pre-existing constraints).
				*/

				Object destConstraint;
				String destName = null;
				String[] destColumnNames = null;

				for (int i=0; i<constraintsVector.size();i++)
				{
					destConstraint = constraintsVector.elementAt(i);
					if (destConstraint instanceof ConstraintDefinitionNode) // match against newly added constraint (create table)
					{
						ConstraintDefinitionNode destCDN = (ConstraintDefinitionNode)destConstraint;
						destName = destCDN.getConstraintMoniker();
						destColumnNames = destCDN.getColumnList().getColumnNames();
						//check if there are multiple constraints with same set of columns

						if (columnsMatch(cdn.getColumnList().getColumnNames(), destColumnNames))
						{
							if(cdn.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
								cdn.setCanBeIgnored(true);
							} else if (destCDN.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
								cdn.setCanBeIgnored(true);
							}
						}

					}
					else if (destConstraint instanceof ConstraintDescriptor) // match against existing constraint (alter table)
					{
						//will come here only for pre-existing constraints in case of alter table
						ConstraintDescriptor destCD = (ConstraintDescriptor)destConstraint;
						destName = destCD.getConstraintName();
						destColumnNames = destCD.getColumnDescriptors().getColumnNames();
						//check if there are multiple constraints with same set of columns
						if (columnsMatch(cdn.getColumnList().getColumnNames(), destColumnNames))
							throw StandardException.newException(SQLState.LANG_MULTIPLE_CONSTRAINTS_WITH_SAME_COLUMNS,
									cdn.getConstraintMoniker(), destName);
					}
				}
				constraintsVector.addElement(cdn);
			}

			/* Make sure that there are no duplicate constraint names in the list */
            checkForDuplicateConstraintNames(ddlStmt, constraintHT, cdn.getConstraintMoniker());

			/* Make sure that the constraint we are trying to drop exists */
			if (cdn.getConstraintType() == DataDictionary.DROP_CONSTRAINT)
			{
				/*
				** If no schema descriptor, then must be an invalid
				** schema name.
				*/

				String dropConstraintName = cdn.getConstraintMoniker();

				if (dropConstraintName != null) {

					String dropSchemaName = cdn.getDropSchemaName();

					SchemaDescriptor sd = dropSchemaName == null ? td.getSchemaDescriptor() :
											getSchemaDescriptor(dropSchemaName);

					ConstraintDescriptor cd =
								dd.getConstraintDescriptorByName(
										td, sd, dropConstraintName,
										false);
					if (cd == null)
					{
						throw StandardException.newException(SQLState.LANG_DROP_NON_EXISTENT_CONSTRAINT,
								(sd.getSchemaName() + "."+ dropConstraintName),
								td.getQualifiedName());
					}
					/* Statement is dependendent on the ConstraintDescriptor */
					getCompilerContext().createDependency(cd);
				}
			}

            // validation of primary key nullability moved to validatePrimaryKeyNullability().
            if (cdn.hasPrimaryKeyConstraint())
            {
                // for PRIMARY KEY, check that columns are unique
                verifyUniqueColumnList(ddlStmt, cdn);
            }
            else if (cdn.hasUniqueKeyConstraint())
            {
                // for UNIQUE, check that columns are unique
                verifyUniqueColumnList(ddlStmt, cdn);
            }
            else if (cdn.hasForeignKeyConstraint())
            {
                // for FOREIGN KEY, check that columns are unique
                verifyUniqueColumnList(ddlStmt, cdn);
            }
		}

		/* Can have only one autoincrement column in DB2 mode */
		if (numAutoCols > 1)
			throw StandardException.newException(SQLState.LANG_MULTIPLE_AUTOINCREMENT_COLUMNS);

	}

    /**
	 * Validate nullability of primary keys. This logic was moved out of the main validate
	 * method so that it can be called after binding generation clauses. We need
	 * to perform the nullability checks later on because the datatype may be
	 * omitted on the generation clause--we can't set/vet the nullability of the
	 * datatype until we determine what the datatype is.
	 */
    public  void    validatePrimaryKeyNullability()
        throws StandardException
    {
		int			size = size();
		for (int index = 0; index < size; index++)
		{
			TableElementNode tableElement = (TableElementNode) elementAt(index);

			if (! (tableElement.hasConstraint()))
			{
				continue;
			}
            
			ConstraintDefinitionNode cdn = (ConstraintDefinitionNode) tableElement;

            if (cdn.hasPrimaryKeyConstraint())
            {
                if (td == null)
                {
                    // in CREATE TABLE so set PRIMARY KEY columns to NOT NULL
                    setColumnListToNotNull(cdn);
                }
                else
                {
                    // in ALTER TABLE so raise error if any columns are nullable
					// delay the check to exeuction time, so that we can allow PK to be added to empty table
					// and change the PK columns to not null even if it is originally defined as nullable
                    // checkForNullColumns(cdn, td);
                }
				// Arrays look ok as primary keys.
				// Add Array Check During Nullability check.
				//validateNoArrays(cdn);
            }
        }
    }
    
    /**
	 * Count the number of constraints of the specified type.
	 *
	 * @param constraintType	The constraint type to search for.
	 *
	 * @return int	The number of constraints of the specified type.
	 */
	public int countConstraints(int constraintType)
	{
		int	numConstraints = 0;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ConstraintDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);

			if (! (element instanceof ConstraintDefinitionNode))
			{
				continue;
			}

			cdn = (ConstraintDefinitionNode) element;

			if (constraintType == cdn.getConstraintType())
			{
				numConstraints++;
			}
		}

		return numConstraints;
	}

    /**
	 * Count the number of generation clauses.
	 */
	public int countGenerationClauses()
	{
		int	numGenerationClauses = 0;
		int size = size();

		for (int index = 0; index < size; index++)
		{
			ColumnDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);

			if (! (element instanceof ColumnDefinitionNode))
			{
				continue;
			}

			cdn = (ColumnDefinitionNode) element;

			if ( cdn.hasGenerationClause() )
			{
				numGenerationClauses++;
			}
		}

		return numGenerationClauses;
	}

	/**
	 * Count the number of columns.
	 *
	 * @return int	The number of columns.
	 */
	public int countNumberOfColumns()
	{
		return numColumns;
	}

	/**
	 * Fill in the ColumnInfo[] for this table element list.
	 * 
	 * @param colInfos	The ColumnInfo[] to be filled in.
	 *
	 * @return int		The number of constraints in the create table.
	 */
	public int genColumnInfos( ColumnInfo[] colInfos, ResultColumnList partitionedColumnList)
        throws StandardException {
		int	numConstraints = 0;
		int size = size();
		List<String> columnNames = Lists.newArrayList(partitionedColumnList==null?new String[0]:partitionedColumnList.getColumnNames());
		for (int index = 0; index < size; index++)
		{
			if (((TableElementNode) elementAt(index)).getElementType() == TableElementNode.AT_DROP_COLUMN)
			{
                String columnName = ((TableElementNode) elementAt(index)).getName();

				colInfos[index] = new ColumnInfo(
								columnName,
								td.getColumnDescriptor( columnName ).getType(),
                                null, null, null, null, null,
								ColumnInfo.DROP, 0, 0, 0,
							-1
						);
				break;
			}

			if (! (elementAt(index) instanceof ColumnDefinitionNode))
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT( elementAt(index) instanceof ConstraintDefinitionNode,
						"elementAt(index) expected to be instanceof " +
						"ConstraintDefinitionNode");
				}

				/* Remember how many constraints we've seen */
				numConstraints++;
				continue;
			}

			ColumnDefinitionNode coldef = (ColumnDefinitionNode) elementAt(index);

            //
            // Generated columns may depend on functions mentioned in their
            // generation clauses.
            //
            ProviderList apl = null;
            ProviderInfo[]	providerInfos = null;
			if ( coldef.hasGenerationClause() )
			{
				apl = coldef.getGenerationClauseNode().getAuxiliaryProviderList();
			}
            if (apl != null && !apl.isEmpty())
            {
                DependencyManager dm = getDataDictionary().getDependencyManager();
                providerInfos = dm.getPersistentProviderInfos(apl);
            }

			colInfos[index - numConstraints] = 
				new ColumnInfo(coldef.getColumnName(),
							   coldef.getType(),
							   coldef.getDefaultValue(),
							   coldef.getDefaultInfo(),
							   providerInfos,
							   (UUID) null,
							   coldef.getOldDefaultUUID(),
							   coldef.getAction(),
							   (coldef.isAutoincrementColumn() ? 
								coldef.getAutoincrementStart() : 0),
							   (coldef.isAutoincrementColumn() ? 
								coldef.getAutoincrementIncrement() : 0),
							   (coldef.isAutoincrementColumn() ? 
								coldef.getAutoinc_create_or_modify_Start_Increment() : -1),
						columnNames.indexOf(coldef.getColumnName()));

			/* Remember how many constraints that we've seen */
			if (coldef.hasConstraint())
			{
				numConstraints++;
			}
		}

		return numConstraints;
	}
	/**
	 * Append goobered up ResultColumns to the table's RCL.
	 * This is useful for binding check constraints for CREATE and ALTER TABLE.
	 *
	 * @param table		The table in question.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void appendNewColumnsToRCL(FromBaseTable table)
		throws StandardException
	{
		int				 size = size();
		ResultColumnList rcl = table.getResultColumns();
		TableName		 exposedName = table.getTableName();

		for (int index = 0; index < size; index++)
		{
			if (elementAt(index) instanceof ColumnDefinitionNode)
			{
				ColumnDefinitionNode cdn = (ColumnDefinitionNode) elementAt(index);
				ResultColumn	resultColumn;
				ValueNode		valueNode;

				/* Build a ResultColumn/BaseColumnNode pair for the column */
				valueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BASE_COLUMN_NODE,
											cdn.getColumnName(),
									  		exposedName,
											cdn.getType(),
											getContextManager());

				resultColumn = (ResultColumn) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN,
												cdn.getType(), 
												valueNode,
												getContextManager());
				resultColumn.setName(cdn.getColumnName());
				rcl.addElement(resultColumn);
			}
		}
	}

	/**
	 * Bind and validate all of the check constraints in this list against
	 * the specified FromList.  
	 *
	 * @param fromList		The FromList in question.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateCheckConstraints(FromList fromList)
		throws StandardException
	{
		CompilerContext cc;
		FromBaseTable				table = (FromBaseTable) fromList.elementAt(0);
		int						  size = size();

		cc = getCompilerContext();

		Vector aggregateVector = new Vector();

		for (int index = 0; index < size; index++)
		{
			ConstraintDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);
			ValueNode	checkTree;

			if (! (element instanceof ConstraintDefinitionNode))
			{
				continue;
			}

			cdn = (ConstraintDefinitionNode) element;

			if (cdn.getConstraintType() != DataDictionary.CHECK_CONSTRAINT)
			{
				continue;
			}

			checkTree = cdn.getCheckCondition();

			// bind the check condition
			// verify that it evaluates to a boolean
			final int previousReliability = cc.getReliability();
			try
			{
				/* Each check constraint can have its own set of dependencies.
				 * These dependencies need to be shared with the prepared
				 * statement as well.  We create a new auxiliary provider list
				 * for the check constraint, "push" it on the compiler context
				 * by swapping it with the current auxiliary provider list
				 * and the "pop" it when we're done by restoring the old 
				 * auxiliary provider list.
				 */
				ProviderList apl = new ProviderList();

				ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
				cc.setCurrentAuxiliaryProviderList(apl);

				// Tell the compiler context to only allow deterministic nodes
				cc.setReliability( CompilerContext.CHECK_CONSTRAINT );
				checkTree = checkTree.bindExpression(fromList, (SubqueryList) null,
										 aggregateVector);

				// no aggregates, please
				if (!aggregateVector.isEmpty())
				{
					throw StandardException.newException(SQLState.LANG_INVALID_CHECK_CONSTRAINT, cdn.getConstraintText());
				}
				
				checkTree = checkTree.checkIsBoolean();
				cdn.setCheckCondition(checkTree);

				/* Save the APL off in the constraint node */
				if (!apl.isEmpty())
				{
					cdn.setAuxiliaryProviderList(apl);
				}

				// Restore the previous AuxiliaryProviderList
				cc.setCurrentAuxiliaryProviderList(prevAPL);
			}
			finally
			{
				cc.setReliability(previousReliability);
			}
	
			/* We have a valid check constraint, now build an array of
			 * 1-based columnIds that the constraint references.
			 */
			ResultColumnList rcl = table.getResultColumns();
			int		numReferenced = rcl.countReferencedColumns();
			int[]	checkColumnReferences = new int[numReferenced];

			rcl.recordColumnReferences(checkColumnReferences, 1);
			cdn.setCheckColumnReferences(checkColumnReferences);

			/* Now we build a list with only the referenced columns and
			 * copy it to the cdn.  Thus we can build the array of
			 * column names for the referenced columns during generate().
			 */
			ResultColumnList refRCL =
						(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
			rcl.copyReferencedColumnsToNewList(refRCL);

			/* A column check constraint can only refer to that column. If this is a
			 * column constraint, we should have an RCL with that column
			 */
			if (cdn.getColumnList() != null)
			{
				String colName = ((ResultColumn)(cdn.getColumnList().elementAt(0))).getName();
				if (numReferenced > 1 ||
					!colName.equals(((ResultColumn)(refRCL.elementAt(0))).getName()))
					throw StandardException.newException(SQLState.LANG_DB2_INVALID_CHECK_CONSTRAINT, colName);
				
			}
			cdn.setColumnList(refRCL);

			/* Clear the column references in the RCL so each check constraint
			 * starts with a clean list.
			 */
			rcl.clearColumnReferences();
		}
	}

	/**
	 * Bind and validate all of the generation clauses in this list against
	 * the specified FromList.  
	 *
	 * @param sd			Schema where the table lives.
	 * @param fromList		The FromList in question.
	 * @param generatedColumns Bitmap of generated columns in the table. Vacuous for CREATE TABLE, but may be non-trivial for ALTER TABLE. This routine may set bits for new generated columns.
	 * @param baseTable  Table descriptor if this is an ALTER TABLE statement.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void bindAndValidateGenerationClauses( SchemaDescriptor sd, FromList fromList, FormatableBitSet generatedColumns, TableDescriptor baseTable )
		throws StandardException
	{
		CompilerContext cc;
		FromBaseTable				table = (FromBaseTable) fromList.elementAt(0);
        ResultColumnList            tableColumns = table.getResultColumns();
        int                                 columnCount = table.getResultColumns().size();
		int						  size = size();

        // complain if a generation clause references another generated column
        findIllegalGenerationReferences( fromList, baseTable );

        generatedColumns.grow( columnCount + 1 );
        
		cc = getCompilerContext();

		List<AggregateNode> aggregateVector = new ArrayList<>();

		for (int index = 0; index < size; index++) {
			ColumnDefinitionNode cdn;
			TableElementNode element = (TableElementNode) elementAt(index);
            GenerationClauseNode    generationClauseNode;
			ValueNode	generationTree;

			if (! (element instanceof ColumnDefinitionNode))
			{
				continue;
			}

			cdn = (ColumnDefinitionNode) element;

			if (!cdn.hasGenerationClause())
			{
				continue;
			}

            generationClauseNode = cdn.getGenerationClauseNode();

			// bind the generation clause
			final int previousReliability = cc.getReliability();
            ProviderList prevAPL = cc.getCurrentAuxiliaryProviderList();
			try
			{
				/* Each generation clause can have its own set of dependencies.
				 * These dependencies need to be shared with the prepared
				 * statement as well.  We create a new auxiliary provider list
				 * for the generation clause, "push" it on the compiler context
				 * by swapping it with the current auxiliary provider list
				 * and the "pop" it when we're done by restoring the old 
				 * auxiliary provider list.
				 */
				ProviderList apl = new ProviderList();

				cc.setCurrentAuxiliaryProviderList(apl);

				// Tell the compiler context to forbid subqueries and
				// non-deterministic functions.
				cc.setReliability( CompilerContext.GENERATION_CLAUSE_RESTRICTION );
				generationTree = generationClauseNode.bindExpression(fromList, (SubqueryList) null,
										 aggregateVector);

                //
                // If the user did not declare a type for this column, then the column type defaults
                // to the type of the generation clause.
                // However, if the user did declare a type for this column, then the
                // type of the generation clause must be assignable to the declared
                // type.
                //
                DataTypeDescriptor  generationClauseType = generationTree.getTypeServices();
                DataTypeDescriptor  declaredType = cdn.getType();
                if ( declaredType == null )
                {
                    cdn.setType( generationClauseType );

                    //
                    // Poke the type into the FromTable so that constraints will
                    // compile.
                    //
                    tableColumns.getResultColumn( cdn.getColumnName(), false ).setType( generationClauseType );

                    //
                    // We skipped these steps earlier on because we didn't have
                    // a datatype. Now that we have a datatype, revisit these
                    // steps.
                    //
                    setCollationTypeOnCharacterStringColumn( sd, cdn );
                    cdn.checkUserType( table.getTableDescriptor() );
                }
                else
                {
                    TypeId  declaredTypeId = declaredType.getTypeId();
                    TypeId  resolvedTypeId = generationClauseType.getTypeId();

                    if ( !getTypeCompiler( resolvedTypeId ).convertible( declaredTypeId, false ) )
                    {
                        throw StandardException.newException
                            ( SQLState.LANG_UNASSIGNABLE_GENERATION_CLAUSE, cdn.getName(), resolvedTypeId.getSQLTypeName() );
                    }
                }

				// no aggregates, please
				if (!aggregateVector.isEmpty())
				{
					throw StandardException.newException( SQLState.LANG_AGGREGATE_IN_GENERATION_CLAUSE, cdn.getName());
				}
				
				/* Save the APL off in the constraint node */
				if (!apl.isEmpty())
				{
					generationClauseNode.setAuxiliaryProviderList(apl);
				}

			}
			finally
			{
				// Restore previous compiler state
				cc.setCurrentAuxiliaryProviderList(prevAPL);
				cc.setReliability(previousReliability);
			}

			/* We have a valid generation clause, now build an array of
			 * 1-based columnIds that the clause references.
			 */
			ResultColumnList rcl = table.getResultColumns();
			int		numReferenced = rcl.countReferencedColumns();
			int[]	generationClauseColumnReferences = new int[numReferenced];
            int     position = rcl.getPosition( cdn.getColumnName(), 1 );

            generatedColumns.set( position );
        
			rcl.recordColumnReferences(generationClauseColumnReferences, 1);

            String[]    referencedColumnNames = new String[ numReferenced ];

            for ( int i = 0; i < numReferenced; i++ )
            {
                referencedColumnNames[ i ] = ((ResultColumn)rcl.elementAt( generationClauseColumnReferences[ i ] - 1 )).getName();
            }

            String              currentSchemaName = getLanguageConnectionContext().getCurrentSchemaName();
            DefaultInfoImpl dii = new DefaultInfoImpl
                ( generationClauseNode.getExpressionText(), referencedColumnNames, currentSchemaName );
            cdn.setDefaultInfo( dii );

			/* Clear the column references in the RCL so each generation clause
			 * starts with a clean list.
			 */
			rcl.clearColumnReferences();
		}

        
	}

	/**
	 * Complain if a generation clause references other generated columns. This
	 * is required by the SQL Standard, part 2, section 4.14.8.
	 *
	 * @param fromList		The FromList in question.
	 * @param baseTable  Table descriptor if this is an ALTER TABLE statement.
	 * @exception StandardException		Thrown on error
	 */
	void findIllegalGenerationReferences( FromList fromList, TableDescriptor baseTable )
		throws StandardException
	{
        ArrayList   generatedColumns = new ArrayList();
        HashSet     names = new HashSet();
		int         size = size();

        // add in existing generated columns if this is an ALTER TABLE statement
        if ( baseTable != null )
        {
            ColumnDescriptorList cdl = baseTable.getGeneratedColumns();
            int                  count = cdl.size();
            for ( int i = 0; i < count; i++ )
            {
                names.add( cdl.elementAt( i ).getColumnName() );
            }
        }
        
        // find all of the generated columns
		for (int index = 0; index < size; index++)
		{
			ColumnDefinitionNode cdn;
			TableElementNode     element = (TableElementNode) elementAt(index);

			if (! (element instanceof ColumnDefinitionNode)) { continue; }

			cdn = (ColumnDefinitionNode) element;

			if (!cdn.hasGenerationClause()) { continue; }

            generatedColumns.add( cdn );
            names.add( cdn.getColumnName() );
        }

        // now look at their generation clauses to see if they reference one
        // another
        int    count = generatedColumns.size();
        for (Object generatedColumn : generatedColumns) {
            ColumnDefinitionNode cdn = (ColumnDefinitionNode) generatedColumn;
            GenerationClauseNode generationClauseNode = cdn.getGenerationClauseNode();
            Vector referencedColumns = generationClauseNode.findReferencedColumns();
            int refCount = referencedColumns.size();
            for (int j = 0; j < refCount; j++) {
                String name = ((ColumnReference) referencedColumns.elementAt(j)).getColumnName();

                if (name != null) {
                    if (names.contains(name)) {
                        throw StandardException.newException(SQLState.LANG_CANT_REFERENCE_GENERATED_COLUMN, cdn.getColumnName());
                    }
                }
            }
        }

    }
    
	/**
	 * Prevent foreign keys on generated columns from violating the SQL spec,
	 * part 2, section 11.8 (<column definition>), syntax rule 12: the
	 * referential action may not specify SET NULL or SET DEFAULT and the update
	 * rule may not specify ON UPDATE CASCADE.  
	 *
	 * @param fromList		The FromList in question.
	 * @param generatedColumns Bitmap of generated columns in the table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void validateForeignKeysOnGenerationClauses(FromList fromList, FormatableBitSet generatedColumns )
		throws StandardException
	{
        // nothing to do if there are no generated columns
        if ( generatedColumns.getNumBitsSet() <= 0 ) { return; }
        
		FromBaseTable				table = (FromBaseTable) fromList.elementAt(0);
        ResultColumnList        tableColumns = table.getResultColumns();
		int						  size = size();

        // loop through the foreign keys, looking for keys which violate the
        // rulse we're enforcing
		for (int index = 0; index < size; index++)
		{
			TableElementNode element = (TableElementNode) elementAt(index);

			if (! (element instanceof FKConstraintDefinitionNode))
			{
				continue;
			}

			FKConstraintDefinitionNode fk = (FKConstraintDefinitionNode) element;
            ConstraintInfo                      ci = fk.getReferencedConstraintInfo();
            int                                     deleteRule = ci.getReferentialActionDeleteRule();
            int                                     updateRule = ci.getReferentialActionUpdateRule();

            //
            // Currently we don't support ON UPDATE CASCADE. Someday we might.
            // We're laying a trip-wire here so that we won't neglect to code the appropriate check
            // when we support ON UPDATE CASCADE.
            //
            if (
                ( updateRule != StatementType.RA_RESTRICT ) &&
                ( updateRule != StatementType.RA_NOACTION )
                )
            {
                throw StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
            }
            
            if (
                ( deleteRule != StatementType.RA_SETNULL ) &&
                ( deleteRule != StatementType.RA_SETDEFAULT )
                )
            { continue; }

            //
            // OK, we have found a foreign key whose referential action is SET NULL or
            // SET DEFAULT or whose update rule is ON UPDATE CASCADE.
            // See if any of the key columns are generated columns.
            //
            ResultColumnList                keyCols = fk.getColumnList();
            int                                     keyCount = keyCols.size();

            for ( int i = 0; i < keyCount; i++ )
            {
                ResultColumn    keyCol = (ResultColumn) keyCols.elementAt( i );
                String                  keyColName = keyCol.getName();
                int     position = tableColumns.getPosition( keyColName, 1 );

                if ( generatedColumns.isSet(  position ) )
                {
                    throw StandardException.newException(SQLState.LANG_BAD_FK_ON_GENERATED_COLUMN, keyColName );
                }
            }

        }   // end of loop through table elements
    }
    
	/**
	 * Fill in the ConstraintConstantAction[] for this create/alter table.
	 * 
     * @param forCreateTable ConstraintConstantAction is for a create table.
	 * @param conActions	The ConstraintConstantAction[] to be filled in.
	 * @param tableName		The name of the Table being created.
	 * @param tableSd		The schema for that table.
	 * @param dd	    	The DataDictionary
	 *
	 * @exception StandardException		Thrown on failure
	 */
	void genConstraintActions(boolean forCreateTable,
				ConstantAction[] conActions,
				String tableName,
				SchemaDescriptor tableSd,
				DataDictionary dd)
		throws StandardException
	{
		int size = size();
		int conActionIndex = 0;
		for (int index = 0; index < size; index++)
		{
			String[]	columnNames = null;
			TableElementNode ten = (TableElementNode) elementAt(index);
			ConstantAction indexAction = null;

			if (! ten.hasConstraint())
			{
				continue;
			}

			if (ten instanceof ColumnDefinitionNode)
			{
				continue;
			}

			ConstraintDefinitionNode constraintDN = (ConstraintDefinitionNode) ten;

			if (constraintDN.getColumnList() != null)
			{
				columnNames = new String[constraintDN.getColumnList().size()];
				constraintDN.getColumnList().exportNames(columnNames);
			}

			int constraintType = constraintDN.getConstraintType();
			String constraintText = constraintDN.getConstraintText();

			/*
			** If the constraint is not named (e.g.
			** create table x (x int primary key)), then
			** the constraintSd is the same as the table.
			*/
			String constraintName = constraintDN.getConstraintMoniker();

			/* At execution time, we will generate a unique name for the backing
			 * index (for CREATE CONSTRAINT) and we will look up the conglomerate
			 * name (for DROP CONSTRAINT).
			 */
			if (constraintDN.requiresBackingIndex())
			{
                // implement unique constraints using a unique backing index 
                // unless it is soft upgrade in version before 10.4, or if 
                // constraint contains no nullable columns.  In 10.4 use 
                // "unique with duplicate null" backing index for constraints 
                // that contain at least one nullable column.

				if (constraintDN.constraintType == DataDictionary.UNIQUE_CONSTRAINT) {
                    boolean contains_nullable_columns = 
                        areColumnsNullable(constraintDN, td);

                    // if all the columns are non nullable, continue to use
                    // a unique backing index.
                    boolean unique = !contains_nullable_columns;

                    // Only use a "unique with duplicate nulls" backing index
                    // for constraints with nullable columns.

					indexAction = genIndexAction(
						forCreateTable,
						unique,
							contains_nullable_columns,
						null, constraintDN,
						columnNames, true, tableSd, tableName,
						constraintType, false,false,dd);
				} 
                else 
                {
					indexAction = genIndexAction(
						forCreateTable,
						constraintDN.requiresUniqueIndex(), false,
						null, constraintDN,
						columnNames, true, tableSd, tableName,
						constraintType, false,false, dd);
				}
			}

			if (constraintType == DataDictionary.DROP_CONSTRAINT)
			{
                if (SanityManager.DEBUG)
                {
                    // Can't drop constraints on a create table.
                    SanityManager.ASSERT(!forCreateTable);
                }
				conActions[conActionIndex] = 
					getGenericConstantActionFactory().
						getDropConstraintConstantAction(
												 constraintName, 
												 constraintDN.getDropSchemaName(), /// FiX
												 tableName,
												 td.getUUID(),
												 tableSd.getSchemaName(),
												 indexAction,
												 constraintDN.getDropBehavior(),
                                                 constraintDN.getVerifyType());
			}
			else
			{
				ProviderList apl = constraintDN.getAuxiliaryProviderList();
				ConstraintInfo refInfo = null;
				ProviderInfo[]	providerInfos = null;

				if (constraintDN instanceof FKConstraintDefinitionNode)
				{
					refInfo = ((FKConstraintDefinitionNode)constraintDN).getReferencedConstraintInfo();
				}				

				/* Create the ProviderInfos, if the constraint is dependent on any Providers */
				if (apl != null && !apl.isEmpty())
				{
					/* Get all the dependencies for the current statement and transfer
					 * them to this view.
					 */
					DependencyManager dm = dd.getDependencyManager();
					providerInfos = dm.getPersistentProviderInfos(apl);
				}
				else
				{
					providerInfos = new ProviderInfo[0];
					// System.out.println("TABLE ELEMENT LIST EMPTY");
				}

				conActions[conActionIndex++] = 
					getGenericConstantActionFactory().
						getCreateConstraintConstantAction(
												 constraintName, 
											     constraintType,
                                                 forCreateTable,
												 tableName, 
												 ((td != null) ? td.getUUID() : (UUID) null),
												 tableSd.getSchemaName(),
												 columnNames,
												 indexAction,
												 constraintText,
												 true, 		// enabled
												 refInfo,
												 providerInfos);
			}
		}
	}

      //check if one array is same as another 
	private boolean columnsMatch(String[] columnNames1, String[] columnNames2)
	{
		int srcCount, srcSize, destCount,destSize;
		boolean match = true;

		if (columnNames1.length != columnNames2.length)
			return false;

		srcSize = columnNames1.length;
		destSize = columnNames2.length;

		for (srcCount = 0; srcCount < srcSize; srcCount++)
		{
			match = false;
			for (destCount = 0; destCount < destSize; destCount++) {
				if (columnNames1[srcCount].equals(columnNames2[destCount])) {
					match = true;
					break;
				}
			}
			if (!match)
				return false;
		}

		return true;
	}

    /**
     * utility to generated the call to create the index.
     * <p>
     *
     *
     * @param forCreateTable                Executed as part of a CREATE TABLE
     * @param isUnique		                True means it will be a unique index
     * @param isUniqueWithDuplicateNulls    True means index check and disallow
     *                                      any duplicate key if key has no 
     *                                      column with a null value.  If any 
     *                                      column in the key has a null value,
     *                                      no checking is done and insert will
     *                                      always succeed.
     * @param indexName	                    The type of index (BTREE, for 
     *                                      example)
     * @param cdn
     * @param columnNames	                Names of the columns in the index,
     *                                      in order.
     * @param isConstraint	                TRUE if index is backing up a 
     *                                      constraint, else FALSE.
     * @param sd
     * @param tableName	                    Name of table the index will be on
     * @param constraintType
     * @param dd
     **/
	private ConstantAction genIndexAction(
    boolean                     forCreateTable,
    boolean                     isUnique,
    boolean                     isUniqueWithDuplicateNulls,
    String                      indexName,
    ConstraintDefinitionNode    cdn,
    String[]                    columnNames,
    boolean                     isConstraint,
    SchemaDescriptor            sd,
    String                      tableName,
    int                         constraintType,
	boolean						excludeNulls,
	boolean 					excludeDefaults,
    DataDictionary              dd)
		throws StandardException
	{
		if (indexName == null) 
        { 
            indexName = cdn.getBackingIndexName(dd); 
        }

		if (constraintType == DataDictionary.DROP_CONSTRAINT)
		{
            if (SanityManager.DEBUG)
            {
                if (forCreateTable)
                    SanityManager.THROWASSERT(
                        "DROP INDEX with forCreateTable true");
            }

			return getGenericConstantActionFactory().getDropIndexConstantAction(
                      null,
                      indexName,
                      tableName,
                      sd.getSchemaName(),
                      td.getUUID(),
                      td.getHeapConglomerateId());
		}
		else
		{
			boolean[]	isAscending = new boolean[columnNames.length];

			for (int i = 0; i < isAscending.length; i++)
				isAscending[i] = true;

			return	getGenericConstantActionFactory().getCreateIndexConstantAction(
                    forCreateTable, 
                    isUnique, 
                    isUniqueWithDuplicateNulls,
                    "BTREE", // indexType
                    sd.getSchemaName(),
                    indexName,
                    tableName,
                    ((td != null) ? td.getUUID() : (UUID) null),
                    columnNames,
                    new DataTypeDescriptor[]{},
                    isAscending,
                    isConstraint,
                    cdn.getBackingIndexUUID(),
					excludeNulls,
					excludeDefaults,
					false,false,false,0,null,null,null,null,null,null,null,
					new String[]{}, new ByteArray[]{}, new String[]{},
                    checkIndexPageSizeProperty(cdn));
		}
	}
    /**
     * Checks if the index should use a larger page size.
     *
     * If the columns in the index are large, and if the user hasn't already
     * specified a page size to use, then we may need to default to the
     * large page size in order to get an index with sufficiently large pages.
     * For example, this DDL should use a larger page size for the index
     * that backs the PRIMARY KEY constraint:
     *
     * create table t (x varchar(1000) primary key)
     *
     * @param cdn Constraint node
     *
     * @return properties to use for creating the index
     */
    private Properties checkIndexPageSizeProperty(ConstraintDefinitionNode cdn) 
        throws StandardException
    {
        Properties result = cdn.getProperties();
        if (result == null)
            result = new Properties();
        if ( result.get(Property.PAGE_SIZE_PARAMETER) != null ||
             PropertyUtil.getServiceProperty(
                 getLanguageConnectionContext().getTransactionCompile(),
                 Property.PAGE_SIZE_PARAMETER) != null)
        {
            // do not override the user's choice of page size, whether it
            // is set for the whole database or just set on this statement.
            return result;
        }
        ResultColumnList rcl = cdn.getColumnList();
        int approxLength = 0;
        for (int index = 0; index < rcl.size(); index++)
        {
            String colName = ((ResultColumn) rcl.elementAt(index)).getName();
            DataTypeDescriptor dtd;
            if (td == null)
                dtd = getColumnDataTypeDescriptor(colName);
            else
                dtd = getColumnDataTypeDescriptor(colName, td);
            // There may be no DTD if the column does not exist. That syntax
            // error is not caught til later in processing, so here we just
            // skip the length checking if the column doesn't exist.
            if (dtd != null)
                approxLength+=dtd.getTypeId().getApproximateLengthInBytes(dtd);
        }
        if (approxLength > Property.IDX_PAGE_SIZE_BUMP_THRESHOLD)
        {
            result.put(
                    Property.PAGE_SIZE_PARAMETER,
                    Property.PAGE_SIZE_DEFAULT_LONG);
        }
        return result;
    }


	/**
	 * Check to make sure that there are no duplicate column names
	 * in the list.  (The comparison here is case sensitive.
	 * The work of converting column names that are not quoted
	 * identifiers to upper case is handled by the parser.)
	 * RESOLVE: This check will also be performed by alter table.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 * @param ht		Hashtable for enforcing uniqueness.
	 * @param colName	Column name to check for.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void checkForDuplicateColumns(DDLStatementNode ddlStmt,
									Hashtable ht,
									String colName)
			throws StandardException
	{
		Object object = ht.put(colName, colName);
		if (object != null)
		{
			/* RESOLVE - different error messages for create and alter table */
			if (ddlStmt instanceof CreateTableNode)
			{
				throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE, colName);
			}
		}
	}

	/**
	 * Check to make sure that there are no duplicate constraint names
	 * in the list.  (The comparison here is case sensitive.
	 * The work of converting column names that are not quoted
	 * identifiers to upper case is handled by the parser.)
	 * RESOLVE: This check will also be performed by alter table.
	 *
	 * @param ddlStmt	DDLStatementNode which contains this list
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void checkForDuplicateConstraintNames(DDLStatementNode ddlStmt,
									Hashtable ht,
									String constraintName)
			throws StandardException
	{
		if (constraintName == null)
			return;

		Object object = ht.put(constraintName, constraintName);
		if (object != null) {

			/* RESOLVE - different error messages for create and alter table */
			if (ddlStmt instanceof CreateTableNode)
			{
				/* RESOLVE - new error message */
				throw StandardException.newException(SQLState.LANG_DUPLICATE_CONSTRAINT_NAME_CREATE, 
						constraintName);
			}
		}
	}

	/**
	 * Verify that a primary/unique table constraint has a valid column list.
	 * (All columns in table and no duplicates.)
	 *
	 * @param ddlStmt	The outer DDLStatementNode
	 * @param cdn		The ConstraintDefinitionNode
	 *
	 * @exception	StandardException	Thrown if the column list is invalid
	 */
	private void verifyUniqueColumnList(DDLStatementNode ddlStmt,
								ConstraintDefinitionNode cdn)
		throws StandardException
	{
		String invalidColName;

		/* Verify that every column in the list appears in the table's list of columns */
		if (ddlStmt instanceof CreateTableNode)
		{
			invalidColName = cdn.getColumnList().verifyCreateConstraintColumnList(this);
			if (invalidColName != null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_CREATE_CONSTRAINT_COLUMN_LIST, 
								ddlStmt.getRelativeName(),
								invalidColName);
			}
		}
		else
		{
			/* RESOLVE - alter table will need to get table descriptor */
		}

		/* Check the uniqueness of the column names within the list */
		invalidColName = cdn.getColumnList().verifyUniqueNames(false);
		if (invalidColName != null)
		{
			throw StandardException.newException(SQLState.LANG_DUPLICATE_CONSTRAINT_COLUMN_NAME, invalidColName);
		}
	}

	/**
	 * Set all columns in that appear in a PRIMARY KEY constraint in a CREATE TABLE statement to NOT NULL.
	 *
	 * @param cdn		The ConstraintDefinitionNode for a PRIMARY KEY constraint
	 */
	private void setColumnListToNotNull(ConstraintDefinitionNode cdn)
	{
		ResultColumnList rcl = cdn.getColumnList();
		int rclSize = rcl.size();
		for (int index = 0; index < rclSize; index++)
		{
			String colName = ((ResultColumn) rcl.elementAt(index)).getName();
            ColumnDefinitionNode colDef = findColumnDefinition(colName);
			colDef.setNullability(false);
        }
	}

	public void validateNoArrays(ConstraintDefinitionNode cdn) throws StandardException {
		ResultColumnList rcl = cdn.getColumnList();
		int rclSize = rcl.size();
		for (int index = 0; index < rclSize; index++)
		{
			String colName = ((ResultColumn) rcl.elementAt(index)).getName();
			ColumnDefinitionNode colDef = findColumnDefinition(colName);
			if (colDef.getType().getTypeId().isArray()) {
				throw StandardException.newException(SQLState.NO_ARRAY_IN_PRIMARY_KEY, colName);
			}
		}
	}


	/**
     * Checks if any of the columns in the constraint can be null.
     *
     * @param cdn Constraint node
     * @param td tabe descriptor of the target table
     *
     * @return true if any of the column can be null false other wise
     */
    private boolean areColumnsNullable (
    ConstraintDefinitionNode    cdn, 
    TableDescriptor             td) 
    {
        ResultColumnList rcl = cdn.getColumnList();
        int rclSize = rcl.size();
        for (int index = 0; index < rclSize; index++)
        {
            String colName = ((ResultColumn) rcl.elementAt(index)).getName();
            DataTypeDescriptor dtd;
            if (td == null)
            {
                dtd = getColumnDataTypeDescriptor(colName);
            }
            else
            {
                dtd = getColumnDataTypeDescriptor(colName, td);
            }
            // todo dtd may be null if the column does not exist, we should check that first
            if (dtd != null && dtd.isNullable())
            {
                return true;
            }
        }
        return false;
    }

    private void checkForNullColumns(ConstraintDefinitionNode cdn, TableDescriptor td) throws StandardException
    {
        ResultColumnList rcl = cdn.getColumnList();
        int rclSize = rcl.size();
        for (int index = 0; index < rclSize; index++)
        {
            String colName = ((ResultColumn) rcl.elementAt(index)).getName();
            DataTypeDescriptor dtd;
            if (td == null)
            {
                dtd = getColumnDataTypeDescriptor(colName);
            }
            else
            {
                dtd = getColumnDataTypeDescriptor(colName, td);
            }
            // todo dtd may be null if the column does not exist, we should check that first
            if (dtd != null && dtd.isNullable()) {
                throw StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_ON_NULL_COLS, colName);
            }
        }
    }

    private DataTypeDescriptor getColumnDataTypeDescriptor(String colName)
    {
        ColumnDefinitionNode col = findColumnDefinition(colName);
        if (col != null)
            return col.getType();

        return null;
    }

    private DataTypeDescriptor getColumnDataTypeDescriptor(String colName, TableDescriptor td)
    {
        // check existing columns
        ColumnDescriptor cd = td.getColumnDescriptor(colName);
        if (cd != null)
        {
            return cd.getType();
        }
        // check for new columns
        return getColumnDataTypeDescriptor(colName);
    }
    
    /**
     * Find the column definition node in this list that matches
     * the passed in column name.
     * @param colName
     * @return Reference to column definition node or null if the column is
     * not in the list.
     */
    public ColumnDefinitionNode findColumnDefinition(String colName) {
        int size = size();
        for (int index = 0; index < size; index++) {
            TableElementNode tableElement = (TableElementNode) elementAt(index);

            if (tableElement instanceof ColumnDefinitionNode) {
                ColumnDefinitionNode cdn = (ColumnDefinitionNode) tableElement;
                if (colName.equals(cdn.getName())) {
                    return cdn;
                }
            }
        }
        return null;
    }
    

	/**
     * Determine whether or not the parameter matches a column name in this
     * list.
     * 
     * @param colName
     *            The column name to search for.
     * 
     * @return boolean Whether or not a match is found.
     */
	public boolean containsColumnName(String colName)
	{
        return findColumnDefinition(colName) != null;
	}


}

