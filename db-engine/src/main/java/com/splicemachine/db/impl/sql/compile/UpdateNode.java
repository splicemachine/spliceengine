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

import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.GenericDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.vti.DeferModification;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Vector;

/**
 * An UpdateNode represents an UPDATE statement.  It is the top node of the
 * query tree for that statement.
 * For positioned update, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 */

public final class UpdateNode extends DMLModStatementNode
{
    public static final String PIN = "pin";
    //Note: These are public so they will be visible to
    //the RepUpdateNode.
    public int[]                changedColumnIds;
    public ExecRow                emptyHeapRow;
    public boolean                deferred;
    public ValueNode            checkConstraints;

    protected FromTable            targetTable;
    protected FormatableBitSet             readColsBitSet;
    protected boolean             positionedUpdate;
    protected boolean             isUpdateWithSubquery; // Splice fork

    /* Column name for the RowLocation in the ResultSet */
    public static final String COLUMNNAME = "###RowLocationToUpdate";

    /**
     * Initializer for an UpdateNode.
     *
     * @param targetTableName    The name of the table to update
     * @param resultSet        The ResultSet that will generate
     *                the rows to update from the given table
     */

    public void init(
               Object targetTableName,
               Object resultSet)
    {
        super.init(resultSet);
        this.targetTableName = (TableName) targetTableName;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return targetTableName.toString() + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "UPDATE";
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            if (targetTableName != null)
            {
                printLabel(depth, "targetTableName: ");
                targetTableName.treePrint(depth + 1);
            }

            /* RESOLVE - need to print out targetTableDescriptor */
        }
    }

    /**
     * Bind this UpdateNode.  This means looking up tables and columns and
     * getting their types, and figuring out the result types of all
     * expressions, as well as doing view resolution, permissions checking,
     * etc.
     * <p>
     * Binding an update will also massage the tree so that
     * the ResultSetNode has a set of columns to contain the old row
     * value, followed by a set of columns to contain the new row
     * value, followed by a column to contain the RowLocation of the
     * row to be updated.
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void bindStatement() throws StandardException
    {
        // We just need select privilege on the expressions
        getCompilerContext().pushCurrentPrivType( Authorizer.SELECT_PRIV);

        FromList    fromList = (FromList) getNodeFactory().getNode(
                                    C_NodeTypes.FROM_LIST,
                                    getNodeFactory().doJoinOrderOptimization(),
                                    getContextManager());
        ResultColumn                rowLocationColumn = null;
        ValueNode                    rowLocationNode = null;
        TableName                    cursorTargetTableName = null;
        CurrentOfNode               currentOfNode = null;
        FromList                    resultFromList;
        ResultColumnList            afterColumns = null;

        DataDictionary dataDictionary = getDataDictionary();

        // check if targetTable is a synonym
        if (targetTableName != null)
        {
            TableName synonymTab = resolveTableToSynonym(this.targetTableName);
            if (synonymTab != null)
            {
                this.synonymTableName = targetTableName;
                this.targetTableName  = synonymTab;
            }
        }

        bindTables(dataDictionary);

        // wait to bind named target table until the cursor
        // binding is done, so that we can get it from the
        // cursor if this is a positioned update.

        // for positioned update, get the cursor's target table.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT((resultSet!=null && resultSet instanceof SelectNode),
                "Update must have a select result set");
        }

        SelectNode sel;
        sel = (SelectNode)resultSet;
        targetTable = (FromTable) sel.fromList.elementAt(0);

        if (targetTable instanceof CurrentOfNode)
        {
            positionedUpdate = true;
            currentOfNode = (CurrentOfNode) targetTable;
            cursorTargetTableName = currentOfNode.getBaseCursorTargetTableName();

            // instead of an assert, we might say the cursor is not updatable.
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(cursorTargetTableName != null);
            }
        }

        if (targetTable instanceof FromVTI)
        {
            targetVTI = (FromVTI) targetTable;
            targetVTI.setTarget();
        }
        else
        {
            // positioned update can leave off the target table.
            // we get it from the cursor supplying the position.
            if (targetTableName == null)
            {
                // verify we have current of
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(cursorTargetTableName!=null);

                targetTableName = cursorTargetTableName;
            }
            // for positioned update, we need to verify that
            // the named table is the same as the cursor's target.
            else if (cursorTargetTableName != null)
            {
                // this match requires that the named table in the update
                // be the same as a correlation name in the cursor.
                if ( !targetTableName.equals(cursorTargetTableName))
                {
                    throw StandardException.newException(SQLState.LANG_CURSOR_UPDATE_MISMATCH,
                        targetTableName,
                        currentOfNode.getCursorName());
                }
            }
        }

        // because we verified that the tables match
        // and we already bound the cursor or the select,
        // the table descriptor should always be found.
        verifyTargetTable();

        /* OVERVIEW - We generate a new ResultColumn, CurrentRowLocation(), and
         * prepend it to the beginning of the source ResultColumnList.  This
         * will tell us which row(s) to update at execution time.  However,
         * we must defer prepending this generated column until the other
         * ResultColumns are bound since there will be no ColumnDescriptor
         * for the generated column.  Thus, the sequence of actions is:
         *
         *        o  Bind existing ResultColumnList (columns in SET clause)
         *        o  If this is a positioned update with a FOR UPDATE OF list,
         *           then verify that all of the target columns are in the
         *           FOR UPDATE OF list.
         *        o  Get the list of indexes that need to be updated.
         *        o  Create a ResultColumnList of all the columns in the target
         *           table - this represents the old row.
         *        o  If we don't know which columns are being updated,
          *           expand the original ResultColumnList to include all the
         *           columns in the target table, and sort it to be in the
         *           order of the columns in the target table.  This represents
         *           the new row.  Append it to the ResultColumnList representing
         *           the old row.
         *        o  Construct the changedColumnIds array sorted by column position.
         *        o  Generate the read column bit map and append any columns
         *           needed for index maint, etc.
         *        o  Generate a new ResultColumn for CurrentRowLocation() and
         *           mark it as a generated column.
         *        o  Append the new ResultColumn to the ResultColumnList
         *           (This must be done before binding the expressions, so
         *           that the proper type info gets propagated to the new
         *           ResultColumn.)
         *        o  Bind the expressions.
         *        o  Bind the generated ResultColumn.
         */

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(fromList.size() == 0,
                "fromList.size() is expected to be 0, not " +
                fromList.size() +
                " on return from RS.bindExpressions()");
        }

        //
        // Add generated columns whose generation clauses mention columns
        // in the user's original update list.
        //
        ColumnDescriptorList    addedGeneratedColumns = new ColumnDescriptorList();
        ColumnDescriptorList    affectedGeneratedColumns = new ColumnDescriptorList();
        addGeneratedColumns
            ( targetTableDescriptor, resultSet, affectedGeneratedColumns, addedGeneratedColumns );
        
        /*
        ** The current result column list is the one supplied by the user.
        ** Mark these columns as "updated", so we can tell later which
        ** columns are really being updated, and which have been added
        ** but are not really being updated.
        */
        resultSet.getResultColumns().markUpdated();

        /* Prepend CurrentRowLocation() to the select's result column list. */
        if (SanityManager.DEBUG)
        SanityManager.ASSERT((resultSet.resultColumns != null),
                              "resultColumns is expected not to be null at bind time");

        /*
        ** Get the result FromTable, which should be the only table in the
         ** from list.
        */
        resultFromList = resultSet.getFromList();
        // Splice fork - don't enforce this if special subquery syntax
        if (!isUpdateWithSubquery) {
        if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultFromList.size() == 1,
            "More than one table in result from list in an update.");
        }

        /* Normalize the SET clause's result column list for synonym */
        if (synonymTableName != null)
            normalizeSynonymColumns( resultSet.resultColumns, targetTable );

        /* Bind the original result columns by column name */
        normalizeCorrelatedColumns( resultSet.resultColumns, targetTable );

        getCompilerContext().pushCurrentPrivType(getPrivType()); // Update privilege
        resultSet.bindResultColumns(targetTableDescriptor,
                    targetVTI,
                    resultSet.resultColumns, this,
                    fromList);
        getCompilerContext().popCurrentPrivType();

        // don't allow overriding of generation clauses
        forbidGenerationOverrides( resultSet.getResultColumns(),
                                   addedGeneratedColumns );
        
        LanguageConnectionContext lcc = getLanguageConnectionContext();
        if (!lcc.getAutoincrementUpdate())
            resultSet.getResultColumns().forbidOverrides(null);

        /*
        ** Mark the columns in this UpdateNode's result column list as
        ** updateable in the ResultColumnList of the table being updated.
        ** only do this for FromBaseTables - if the result table is a
        ** CurrentOfNode, it already knows what columns in its cursor
        ** are updateable.
        */
        boolean allColumns = false;
        if (targetTable instanceof FromBaseTable)
        {
            ((FromBaseTable) targetTable).markUpdated(
                                                resultSet.getResultColumns());
        }
        else if (targetTable instanceof FromVTI)
        {
            resultColumnList = resultSet.getResultColumns();
        }
        else
        {
            /*
            ** Positioned update: WHERE CURRENT OF
            */
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(currentOfNode != null, "currentOfNode is null");
            }

            ExecPreparedStatement     cursorStmt = currentOfNode.getCursorStatement();
            String[] ucl = cursorStmt.getUpdateColumns();

            /*
            ** If there is no update column list, we need to build
            ** out the result column list to have all columns.
            */
            if (ucl == null || (ucl.length == 0))
            {
                /*
                ** Get the resultColumnList representing ALL of the columns in the
                ** base table.  This is the "before" portion of the result row.
                */
                getResultColumnList();

                /*
                ** Add the "after" portion of the result row.  This is the update
                ** list augmented to include every column in the target table.
                ** Those columns that are not being updated are set to themselves.
                ** The expanded list will be in the order of the columns in the base
                ** table.
                */
                afterColumns = resultSet.getResultColumns().expandToAll(
                                                    targetTableDescriptor,
                                                    targetTable.getTableName());

                /*
                ** Need to get all indexes here since we aren't calling
                ** getReadMap().
                */
                getAffectedIndexes(targetTableDescriptor,
                                    (ResultColumnList)null, (FormatableBitSet)null);
                allColumns = true;
            }
            else
            {
                /* Check the updatability */
                resultSet.getResultColumns().checkColumnUpdateability(ucl,
                                currentOfNode.getCursorName());
            }
        }

        changedColumnIds = getChangedColumnIds(resultSet.getResultColumns());

        /*
        ** We need to add in all the columns that are needed
        ** by the constraints on this table.
        */
        if (!allColumns && targetVTI == null)
        {
            getCompilerContext().pushCurrentPrivType( Authorizer.NULL_PRIV);
            try
            {
                readColsBitSet = new FormatableBitSet();
                FromBaseTable fbt = getResultColumnList(resultSet.getResultColumns());
                afterColumns = resultSet.getResultColumns().copyListAndObjects();

                readColsBitSet = getReadMap(dataDictionary,
                                        targetTableDescriptor,
                                        afterColumns, affectedGeneratedColumns );

                afterColumns = fbt.addColsToList(afterColumns, readColsBitSet);
                resultColumnList = fbt.addColsToList(resultColumnList, readColsBitSet);

                /*
                ** If all bits are set, then behave as if we chose all
                ** in the first place
                */
                int i = 1;
                int size = targetTableDescriptor.getMaxColumnID();
                for (; i <= size; i++)
                {
                    if (!readColsBitSet.get(i))
                    {
                        break;
                    }
                }

                if (i > size)
                {
                    readColsBitSet = null;
                    allColumns = true;
                }
            }
            finally
            {
                getCompilerContext().popCurrentPrivType();
            }
        }

        if (targetVTI == null)
        {
            /*
            ** Construct an empty heap row for use in our constant action.
            */
            emptyHeapRow = targetTableDescriptor.getEmptyExecRow();

            /* Append the list of "after" columns to the list of "before" columns,
             * preserving the afterColumns list.  (Necessary for binding
             * check constraints.)
             */
            resultColumnList.appendResultColumns(afterColumns, false);

            /* Generate the RowLocation column */
            rowLocationNode = (ValueNode) getNodeFactory().getNode(
                                        C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
                                        getContextManager());
        }
        else
        {
            rowLocationNode = (ValueNode) getNodeFactory().getNode(
                                        C_NodeTypes.INT_CONSTANT_NODE,
                                        ReuseFactory.getInteger( 0),
                                        getContextManager());
        }


        /*
         * The last thing that we do to the generated RCL is to clear
         * the table name out from each RC. See comment on
         * checkTableNameAndScrubResultColumns().
         */
        // Splice fork: leave table names present in special case
        // of multi column update with single source select.
        checkTableNameAndScrubResultColumns(resultColumnList,isUpdateWithSubquery);


        /* Set the new result column list in the result set */
        resultSet.setResultColumns(resultColumnList);

        /* Bind the expressions */
        getCompilerContext().pushCurrentPrivType(getPrivType()); // Update privilege
        super.bindExpressions();
        getCompilerContext().popCurrentPrivType();

        ResultColumn rowIdColumn = targetTable.getRowIdColumn();
        if (rowIdColumn == null) {
            rowIdColumn =
                    (ResultColumn) getNodeFactory().getNode(
                            C_NodeTypes.RESULT_COLUMN,
                            COLUMNNAME,
                            rowLocationNode,
                            getContextManager());
            rowLocationNode.setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.REF_NAME),
                            false        /* Not nullable */
                    )
            );
            rowIdColumn.markGenerated();
            targetTable.setRowIdColumn(rowIdColumn);
        } else {
            rowIdColumn.setName(COLUMNNAME);
        }

        ColumnReference columnReference = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                rowIdColumn.getName(),
                null,
                getContextManager());
        columnReference.setSource(rowIdColumn);
        columnReference.setNestingLevel(targetTable.getLevel());
        columnReference.setSourceLevel(targetTable.getLevel());
        rowLocationColumn =
                (ResultColumn) getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN,
                        COLUMNNAME,
                        columnReference,
                        getContextManager());

        /* Append to the ResultColumnList */
        resultColumnList.addResultColumn(rowLocationColumn);

        /* Bind untyped nulls directly under the result columns */
        resultSet.
            getResultColumns().
                bindUntypedNullsToResultColumns(resultColumnList);

        if (null != rowLocationColumn)
        {
            /* Bind the new ResultColumn */
            rowLocationColumn.bindResultColumnToExpression();
        }

        resultColumnList.checkStorableExpressions();

        /* Insert a NormalizeResultSetNode above the source if the source
         * and target column types and lengths do not match.
         */
        if (! resultColumnList.columnTypesAndLengthsMatch())
         {
            resultSet = (ResultSetNode) getNodeFactory().getNode(
                C_NodeTypes.NORMALIZE_RESULT_SET_NODE,
                resultSet, resultColumnList, null, Boolean.TRUE,
                getContextManager());


             if (hasCheckConstraints(dataDictionary, targetTableDescriptor) || hasGenerationClauses( targetTableDescriptor ) )
             {
                 /* Get and bind all check constraints and generated columns on the columns
                  * being updated.  We want to bind the check constraints and
                  * generated columns against
                  * the after columns.  We need to bind against the portion of the
                  * resultColumns in the new NormalizeResultSet that point to
                  * afterColumns.  Create an RCL composed of just those RCs in
                  * order to bind the check constraints.
                  */
                 int afterColumnsSize = afterColumns.size();
                 afterColumns = (ResultColumnList) getNodeFactory().getNode(
                                                C_NodeTypes.RESULT_COLUMN_LIST,
                                                getContextManager());
                 ResultColumnList normalizedRCs = resultSet.getResultColumns();
                 for (int index = 0; index < afterColumnsSize; index++)
                 {
                     afterColumns.addElement(normalizedRCs.elementAt(index + afterColumnsSize));
                 }
            }
        }

        if( null != targetVTI)
        {
            deferred = VTIDeferModPolicy.deferIt( DeferModification.UPDATE_STATEMENT,
                                                  targetVTI,
                                                  resultColumnList.getColumnNames(),
                                                  sel.getWhereClause());
        }
        else // not VTI
        {
            /* we always include triggers in core language */
            boolean hasTriggers = (getAllRelevantTriggers(dataDictionary, targetTableDescriptor, 
                                                          changedColumnIds, true).size() > 0);

            ResultColumnList sourceRCL = hasTriggers ? resultColumnList : afterColumns;

            /* bind all generation clauses for generated columns */
            parseAndBindGenerationClauses
                ( dataDictionary, targetTableDescriptor, afterColumns, resultColumnList, true, resultSet );

            /* Get and bind all constraints on the columns being updated */
            checkConstraints = bindConstraints( dataDictionary,
                                                getNodeFactory(),
                                                targetTableDescriptor,
                                                null,
                                                sourceRCL,
                                                changedColumnIds,
                                                readColsBitSet,
                                                false,
                                                true); /* we always include triggers in core language */

            /* If the target table is also a source table, then
             * the update will have to be in deferred mode
             * For updates, this means that the target table appears in a
             * subquery.  Also, self referencing foreign keys are
             * deferred.  And triggers cause an update to be deferred.
             */
            if (resultSet.subqueryReferencesTarget(
                targetTableDescriptor.getName(), true) ||
                requiresDeferredProcessing())
            {
                deferred = true;
            }
        }

        getCompilerContext().popCurrentPrivType();

    } // end of bind()

    int getPrivType()
    {
        return Authorizer.UPDATE_PRIV;
    }

    void setUpdateWithSubquery(boolean withSubquery) {
        isUpdateWithSubquery = withSubquery;
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @return    true if references SESSION schema tables, else false
     *
     * @exception StandardException        Thrown on error
     */
    public boolean referencesSessionSchema()
        throws StandardException
    {
        //If this node references a SESSION schema table, then return true.
        return(resultSet.referencesSessionSchema());

    }

    /**
     * Compile constants that Execution will use
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction    makeConstantAction() throws StandardException
    {
        /*
        ** Updates are also deferred if they update a column in the index
        ** used to scan the table being updated.
        */
        if (! deferred )
        {
            /**
             * When the underneath SelectNode is unsatisfiable, we rewrite it to
             * a RowResultSetNode representing "values (null, null, ..., null) where 1=0"
             * In such a case, it is possible that the target table does not participate in planning and thus
             * its accessPath remains as null
             */
            AccessPath accessPath = targetTable.getTrulyTheBestAccessPath();
            ConglomerateDescriptor updateCD = accessPath==null?null:accessPath.getConglomerateDescriptor();

            if (updateCD != null && updateCD.isIndex())
            {
                int [] baseColumns =
                        updateCD.getIndexDescriptor().baseColumnPositions();

                if (resultSet.
                        getResultColumns().
                                        updateOverlaps(baseColumns))
                {
                    deferred = true;
                }
            }
        }

        if( null == targetTableDescriptor)
        {
            /* Return constant action for VTI
             * NOTE: ConstantAction responsible for preserving instantiated
             * VTIs for in-memory queries and for only preserving VTIs
             * that implement Serializable for SPSs.
             */
            return    getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.UPDATE_STATEMENT,
                        deferred, changedColumnIds);
        }

        int lockMode = resultSet.updateTargetLockMode();
        long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
        TransactionController tc =
            getLanguageConnectionContext().getTransactionCompile();
        StaticCompiledOpenConglomInfo[] indexSCOCIs =
            new StaticCompiledOpenConglomInfo[indexConglomerateNumbers.length];

        for (int index = 0; index < indexSCOCIs.length; index++)
        {
            indexSCOCIs[index] = tc.getStaticCompiledConglomInfo(indexConglomerateNumbers[index]);
        }

        /*
        ** Do table locking if the table's lock granularity is
        ** set to table.
        */
        if (targetTableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY)
        {
            lockMode = TransactionController.MODE_TABLE;
        }


        return    getGenericConstantActionFactory().getUpdateConstantAction
            ( heapConglomId,
              targetTableDescriptor.getTableType(),
              tc.getStaticCompiledConglomInfo(heapConglomId),
              pkColumns,
              indicesToMaintain,
              indexConglomerateNumbers,
              indexSCOCIs,
              indexNames,
              emptyHeapRow,
              deferred,
              targetTableDescriptor.getUUID(),
              lockMode,
              false,
              changedColumnIds, null, null,
              getFKInfo(),
              getTriggerInfo(),
              (readColsBitSet == null) ? (FormatableBitSet)null : new FormatableBitSet(readColsBitSet),
              getReadColMap(targetTableDescriptor.getNumberOfColumns(),readColsBitSet),
              resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
              (readColsBitSet == null) ?
                  targetTableDescriptor.getNumberOfColumns() :
                  readColsBitSet.getNumBitsSet(),
              positionedUpdate,
              resultSet.isOneRowResultSet(),
              targetTableDescriptor.getStoragePositionArray()
              );
    }

    /**
     * Updates are deferred if they update a column in the index
     * used to scan the table being updated.
     */
    protected void setDeferredForUpdateOfIndexColumn()
    {
        /* Don't bother checking if we're already deferred */
        if (! deferred )
        {
            /* Get the conglomerate descriptor for the target table */
            ConglomerateDescriptor updateCD =
                                        targetTable.
                                            getTrulyTheBestAccessPath().
                                                getConglomerateDescriptor();

            /* If it an index? */
            if (updateCD != null && updateCD.isIndex())
            {
                int [] baseColumns =
                        updateCD.getIndexDescriptor().baseColumnPositions();

                /* Are any of the index columns updated? */
                if (resultSet.
                        getResultColumns().
                                        updateOverlaps(baseColumns))
                {
                    deferred = true;
                }
            }
        }
    }

    /**
     * Code generation for update.
     * The generated code will contain:
     *        o  A static member for the (xxx)ResultSet with the RowLocations    and
     *           new update values
     *        o  The static member will be assigned the appropriate ResultSet within
     *           the nested calls to get the ResultSets.  (The appropriate cast to the
     *           (xxx)ResultSet will be generated.)
     *        o  The CurrentRowLocation() in SelectNode's select list will generate
     *           a new method for returning the RowLocation as well as a call to
     *           that method when generating the (xxx)ResultSet.
     *
     * @param acb    The ActivationClassBuilder for the class being built
     * @param mb    The method for the execute() method to be built
     *
     *
     * @exception StandardException        Thrown on error
     */
    public void generate(ActivationClassBuilder acb,
                                MethodBuilder mb)
                            throws StandardException
    {
        // If the DML is on the temporary table, generate the code to
        // mark temporary table as modified in the current UOW. After
        // DERBY-827 this must be done in execute() since
        // fillResultSet() will only be called once.
        generateCodeForTemporaryTable(acb, acb.getExecuteMethod());

        /* generate the parameters */
        if(!isDependentTable)
            generateParameterValueSet(acb);


        /* Create the static declaration for the scan ResultSet which generates the
         * RowLocations to be updated
         * RESOLVE - Need to deal with the type of the static member.
         */
        acb.newFieldDeclaration(Modifier.PRIVATE,
                                ClassName.CursorResultSet,
                                acb.newRowLocationScanResultSetName());

        /*
        ** Generate the update result set, giving it either the original
        ** source or the normalize result set, the constant action.
        */

        acb.pushGetResultSetFactoryExpression(mb);
        resultSet.generate(acb, mb); // arg 1

        if( null != targetVTI)
        {
            targetVTI.assignCostEstimate(resultSet.getNewCostEstimate());
            mb.push((double)this.resultSet.getFinalCostEstimate(false).getEstimatedRowCount());
            mb.push(this.resultSet.getFinalCostEstimate(false).getEstimatedCost());
            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateVTIResultSet", ClassName.ResultSet, 3);
        }
        else
        {
            // arg 2 generate code to evaluate generation clauses
            generateGenerationClauses( resultColumnList, resultSet.getResultSetNumber(), true, acb, mb );

            // generate code to evaluate CHECK CONSTRAINTS
            generateCheckConstraints( checkConstraints, acb, mb ); // arg 3

            if(isDependentTable)
            {
                mb.push(acb.addItem(makeConstantAction()));
                mb.push(acb.addItem(makeResultDescription()));
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getDeleteCascadeUpdateResultSet",
                              ClassName.ResultSet, 5);
            }else
            {
                mb.push((double)this.resultSet.getFinalCostEstimate(false).getEstimatedRowCount());
                mb.push(this.resultSet.getFinalCostEstimate(false).getEstimatedCost());
                mb.push(targetTableDescriptor.getVersion());
                mb.push(this.printExplainInformationForActivation());
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateResultSet",
                              ClassName.ResultSet, 7);
            }
        }
    }

    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected final int getStatementType()
    {
        return StatementType.UPDATE;
    }


    /**
     * Gets the map of all columns which must be read out of the base table.
     * These are the columns needed to<UL>:
     *        <LI>maintain indices</LI>
     *        <LI>maintain foreign keys</LI>
     *        <LI>maintain generated columns</LI>
     *        <LI>support Replication's Delta Optimization</LI></UL>
     * <p>
     * The returned map is a FormatableBitSet with 1 bit for each column in the
     * table plus an extra, unsued 0-bit. If a 1-based column id must
     * be read from the base table, then the corresponding 1-based bit
     * is turned ON in the returned FormatableBitSet.
     * <p>
     * <B>NOTE</B>: this method is not expected to be called when
     * all columns are being updated (i.e. updateColumnList is null).
     *
     * @param dd                the data dictionary to look in
     * @param baseTable        the base table descriptor
     * @param updateColumnList the rcl for the update. CANNOT BE NULL
     * @param affectedGeneratedColumns columns whose generation clauses mention columns being updated
     *
     * @return a FormatableBitSet of columns to be read out of the base table
     *
     * @exception StandardException        Thrown on error
     */
    public    FormatableBitSet    getReadMap
    (
        DataDictionary        dd,
        TableDescriptor        baseTable,
        ResultColumnList    updateColumnList,
        ColumnDescriptorList    affectedGeneratedColumns
    )
        throws StandardException
    {
        boolean[]    needsDeferredProcessing = new boolean[1];
        needsDeferredProcessing[0] = requiresDeferredProcessing();

        Vector        conglomVector = new Vector();
        relevantCdl = new ConstraintDescriptorList();
        relevantTriggers =  new GenericDescriptorList();

        FormatableBitSet    columnMap = getUpdateReadMap
            (
             dd, baseTable, updateColumnList, conglomVector, relevantCdl,
             relevantTriggers, needsDeferredProcessing, affectedGeneratedColumns );

        markAffectedIndexes( conglomVector );

        adjustDeferredFlag( needsDeferredProcessing[0] );

        return    columnMap;
    }


    /**
     * Construct the changedColumnIds array. Note we sort its entries by
     * columnId.
     */
    private int[] getChangedColumnIds(ResultColumnList rcl)
    {
        if (rcl == null) { return (int[])null; }
        else { return rcl.sortMe(); }
    }
    /**
      *    Builds a bitmap of all columns which should be read from the
      *    Store in order to satisfy an UPDATE statement.
      *
      *    Is passed a list of updated columns. Does the following:
      *
      *    1)    finds all indices which overlap the updated columns
      *    2)    adds the index columns to a bitmap of affected columns
      *    3)    adds the index descriptors to a list of conglomerate
      *        descriptors.
      *    4)    finds all constraints which overlap the updated columns
      *        and adds the constrained columns to the bitmap
      *    5)    finds all triggers which overlap the updated columns.
      *    6)    Go through all those triggers from step 5 and for each one of
      *     those triggers, follow the rules below to decide which columns
      *     should be read.
      *       Rule1)If trigger column information is null, then read all the
      *       columns from trigger table into memory irrespective of whether
      *       there is any trigger action column information. 2 egs of such
      *       triggers
      *         create trigger tr1 after update on t1 for each row values(1);
      *         create trigger tr1 after update on t1 referencing old as oldt
      *             for each row insert into t2 values(2,oldt.j,-2);
      *       Rule2)If trigger column information is available but no trigger
      *       action column information is found and no REFERENCES clause is
      *       used for the trigger, then read all the columns identified by
      *       the trigger column. eg
      *         create trigger tr1 after update of c1 on t1
      *             for each row values(1);
      *       Rule3)If trigger column information and trigger action column
      *       information both are not null, then only those columns will be
      *       read into memory. This is possible only for triggers created in
      *       release 10.9 or higher(with the exception of 10.7.1.1 where we
      *       did collect that information but because of corruption caused
      *       by those changes, we do not use the information collected by
      *       10.7). Starting 10.9, we are collecting trigger action column
      *       informatoin so we can be smart about what columns get read
      *       during trigger execution. eg
      *         create trigger tr1 after update of c1 on t1
      *             referencing old as oldt for each row
      *             insert into t2 values(2,oldt.j,-2);
      *       Rule4)If trigger column information is available but no trigger
      *       action column information is found but REFERENCES clause is used
      *       for the trigger, then read all the columns from the trigger
      *       table. This will cover soft-upgrade scenario for triggers created
      *       pre-10.9.
      *       eg trigger created prior to 10.9
      *         create trigger tr1 after update of c1 on t1
      *             referencing old as oldt for each row
      *             insert into t2 values(2,oldt.j,-2);
      *    7)    adds the triggers to an evolving list of triggers
      *    8)    finds all generated columns whose generation clauses mention
      *        the updated columns and adds all of the mentioned columns
      *
      *    @param    dd    Data Dictionary
      *    @param    baseTable    Table on which update is issued
      *    @param    updateColumnList    a list of updated columns
      *    @param    conglomVector        OUT: vector of affected indices
      *    @param    relevantConstraints    IN/OUT. Empty list is passed in. We hang constraints on it as we go.
      *    @param    relevantTriggers    IN/OUT. Passed in as an empty list. Filled in as we go.
      *    @param    needsDeferredProcessing    IN/OUT. true if the statement already needs
      *                                    deferred processing. set while evaluating this
      *                                    routine if a trigger or constraint requires
      *                                    deferred processing
      *    @param    affectedGeneratedColumns columns whose generation clauses mention updated columns
      *
      * @return a FormatableBitSet of columns to be read out of the base table
      *
      * @exception StandardException        Thrown on error
      */
    public static FormatableBitSet getUpdateReadMap
    (
        DataDictionary        dd,
        TableDescriptor                baseTable,
        ResultColumnList            updateColumnList,
        Vector                        conglomVector,
        ConstraintDescriptorList    relevantConstraints,
        GenericDescriptorList        relevantTriggers,
        boolean[]                    needsDeferredProcessing,
        ColumnDescriptorList    affectedGeneratedColumns
    )
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(updateColumnList != null, "updateColumnList is null");
        }

        int        columnCount = baseTable.getMaxColumnID();
        FormatableBitSet    columnMap = new FormatableBitSet(columnCount + 1);

        /*
        ** Add all the changed columns.  We don't strictly
        ** need the before image of the changed column in all cases,
        ** but it makes life much easier since things are set
        ** up around the assumption that we have the before
        ** and after image of the column.
        */
        int[]    changedColumnIds = updateColumnList.sortMe();

        for (int changedColumnId : changedColumnIds) {
            columnMap.set(changedColumnId);
        }

        /*
        ** Get a list of the indexes that need to be
        ** updated.  ColumnMap contains all indexed
        ** columns where 1 or more columns in the index
        ** are going to be modified.
        */
        DMLModStatementNode.getXAffectedIndexes(baseTable, updateColumnList, columnMap, conglomVector, false);
 
        /*
        ** Add all columns needed for constraints.  We don't
        ** need to bother with foreign key/primary key constraints
        ** because they are added as a side effect of adding
        ** their indexes above.
        */
        baseTable.getAllRelevantConstraints
            ( StatementType.UPDATE, false, changedColumnIds, needsDeferredProcessing, relevantConstraints );

        int rclSize = relevantConstraints.size();
        for (int index = 0; index < rclSize; index++)
        {
            ConstraintDescriptor cd = relevantConstraints.elementAt(index);
            if (cd.getConstraintType() != DataDictionary.CHECK_CONSTRAINT)
            {
                continue;
            }

            int[] refColumns = cd.getReferencedColumns();
            for (int refColumn : refColumns) {
                columnMap.set(refColumn);
            }
        }

        //
        // Add all columns mentioned by generation clauses which are affected
        // by the columns being updated.
        //
        addGeneratedColumnPrecursors( baseTable, affectedGeneratedColumns, columnMap );
        
        /*
         * If we have any UPDATE triggers, then we will follow the 4 rules
         * mentioned in the comments at the method level.
         */
        baseTable.getAllRelevantTriggers( StatementType.UPDATE, changedColumnIds, relevantTriggers );

        if (relevantTriggers.size() > 0)
        {
            needsDeferredProcessing[0] = true;

            boolean needToIncludeAllColumns = false;
            // If we are dealing with database created in 10.8 and prior,
            // then we must be in soft upgrade mode. For such databases,
            // we do not want to do any column reading optimization.
            //
            // For triggers created in 10.7.1.1, we kept track of trigger
            // action columns used through the REFERENCING clause. That
            // information was gathered so we could be smart about what
            // columns from trigger table should be read during trigger
            // execution. But those changes in code resulted in data
            // corruption DERBY-5121. Because of that, we took out the
            // column read optimization changes from codeline for next
            // release of 10.7 and 10.8 codeline.
            // But we can still have triggers created in 10.7.1.1 with
            // trigger action column information in SYSTRIGGERS.
            // In 10.9, we are reimplementing what columns should be read
            // from the trigger table during trigger execution. But we do
            // not want this column optimization changes to be used in soft
            // upgrade mode for a 10.8 or prior database so that we can
            // go back to the older release if that's what the user chooses
            // after the soft-upgrade.
            for (Object relevantTrigger : relevantTriggers) {
                TriggerDescriptor trd = (TriggerDescriptor) relevantTrigger;

                // See if we can avoid reading all the columns from the
                // trigger table.
                int[] referencedColsInTriggerAction = trd.getReferencedColsInTriggerAction();
                int[] triggerCols = trd.getReferencedCols();
                if (triggerCols == null || triggerCols.length == 0) {
                    for (int i = 0; i < columnCount; i++) {
                        columnMap.set(i + 1);
                    }
                    //This trigger is not defined on specific columns
                    // so we will have to read all the columns from the
                    // trigger table. Now, there is no need to go
                    // through the rest of the triggers because we are
                    // going to read all the columns anyways.
                    break;
                } else {
                    if (referencedColsInTriggerAction == null ||
                            referencedColsInTriggerAction.length == 0) {
                        //Does this trigger have REFERENCING clause defined on it
                        if (!trd.getReferencingNew() && !trd.getReferencingOld()) {
                            //The trigger does not use trigger action columns through
                            //the REFERENCING clause so we need to read just the
                            //trigger columns
                            for (int triggerCol : triggerCols) {
                                columnMap.set(triggerCol);
                            }
                        } else {
                            //The trigger has REFERENCING clause defined on it
                            // so it might be used them in trigger action.
                            // We should just go ahead and read all the
                            // columns from the trigger table. Now, there is
                            // no need to go through the rest of the triggers
                            // because we are going to read all the columns
                            // anyways.
                            needToIncludeAllColumns = true;
                            break;
                        }
                    } else {
                        //This trigger has both trigger columns and
                        // trigger action columns(getting used through
                        // the REFERENCING clause). Read only those
                        // columns because that's all we need from
                        // trigger table for the trigger execution.
                        for (int triggerCol : triggerCols) {
                            columnMap.set(triggerCol);
                        }
                        for (int aReferencedColsInTriggerAction : referencedColsInTriggerAction) {
                            columnMap.set(aReferencedColsInTriggerAction);
                        }
                    }
                }

            }

        if (needToIncludeAllColumns) {
                for (int i = 1; i <= columnCount; i++)
                {
                        columnMap.set(i);
                }
            }

        }

        return    columnMap;
    }

    /**
     * Add all of the columns mentioned by the generation clauses of generated
     * columns. The generated columns were added when we called
     * addGeneratedColumns earlier on.
     */
    private static  void    addGeneratedColumnPrecursors
    (
     TableDescriptor         baseTable,
     ColumnDescriptorList    affectedGeneratedColumns,
     FormatableBitSet        columnMap
    )
        throws StandardException
    {
        int                                 generatedColumnCount = affectedGeneratedColumns.size();
        
        for ( int gcIdx = 0; gcIdx < generatedColumnCount; gcIdx++ )
        {
            ColumnDescriptor    gc = affectedGeneratedColumns.elementAt( gcIdx );
            String[]                       mentionedColumnNames = gc.getDefaultInfo().getReferencedColumnNames();
            int[]                       mentionedColumns = baseTable.getColumnIDs( mentionedColumnNames );
            int                         mentionedColumnCount = mentionedColumns.length;

            for (int mentionedColumn : mentionedColumns) {
                columnMap.set(mentionedColumn);

            }   // done looping through mentioned columns
            
        }   // done looping through affected generated columns

    }
     
    /**
     * Add generated columns to the update list as necessary. We add
     * any column whose generation clause mentions columns already
     * in the update list. We fill in a list of all generated columns affected
     * by this update. We also fill in a list of all generated columns which we
     * added to the update list.
     */
    private void    addGeneratedColumns
    (
        TableDescriptor                baseTable,
        ResultSetNode               updateSet,
        ColumnDescriptorList    affectedGeneratedColumns,
        ColumnDescriptorList    addedGeneratedColumns
    )
        throws StandardException
    {
        ResultColumnList        updateColumnList = updateSet.getResultColumns();
        int                     count = updateColumnList.size();
        ColumnDescriptorList    generatedColumns = baseTable.getGeneratedColumns();
        int                     generatedColumnCount = generatedColumns.size();
        HashSet                 updatedColumns = new HashSet();
        UUID                    tableID = baseTable.getObjectID();
        
        for (int ix = 0; ix < count; ix++)
        {
            String      name = ((ResultColumn)updateColumnList.elementAt( ix )).getName();

            updatedColumns.add( name );
        }

        for ( int gcIdx = 0; gcIdx < generatedColumnCount; gcIdx++ )
        {
            ColumnDescriptor    gc = generatedColumns.elementAt( gcIdx );
            DefaultInfo             defaultInfo = gc.getDefaultInfo();
            String[]                       mentionedColumnNames = defaultInfo.getReferencedColumnNames();
            int                         mentionedColumnCount = mentionedColumnNames.length;

            // handle the case of setting a generated column to the DEFAULT
            // literal
            if ( updatedColumns.contains( gc.getColumnName() ) ) { affectedGeneratedColumns.add( tableID, gc ); }

            // figure out if this generated column is affected by the
            // update
            for (String mentionedColumnName : mentionedColumnNames) {
                if (updatedColumns.contains(mentionedColumnName)) {
                    // Yes, we are updating one of the columns mentioned in
                    // this generation clause.
                    affectedGeneratedColumns.add(tableID, gc);

                    // If the generated column isn't in the update list yet,
                    // add it.
                    if (!updatedColumns.contains(gc.getColumnName())) {
                        addedGeneratedColumns.add(tableID, gc);

                        // we will fill in the real value later on in parseAndBindGenerationClauses();
                        ValueNode dummy = (ValueNode) getNodeFactory().getNode
                                (C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE, getContextManager());
                        ResultColumn newResultColumn = (ResultColumn) getNodeFactory().getNode
                                (C_NodeTypes.RESULT_COLUMN, gc.getType(), dummy, getContextManager());
                        newResultColumn.setColumnDescriptor(baseTable, gc);
                        newResultColumn.setName(gc.getColumnName());

                        updateColumnList.addResultColumn(newResultColumn);
                    }

                    break;
                }
            }   // done looping through mentioned columns

        }   // done looping through generated columns
    }
     

    /*
     * Force correlated column references in the SET clause to have the
     * name of the base table. This dances around the problem alluded to
     * in scrubResultColumn().
     */
    private    void    normalizeCorrelatedColumns( ResultColumnList rcl, FromTable fromTable )
        throws StandardException
    {
        String        correlationName = fromTable.getCorrelationName();

        if ( correlationName == null ) { return; }

        TableName    tableNameNode;

        if ( fromTable instanceof CurrentOfNode )
        { tableNameNode = ((CurrentOfNode) fromTable).getBaseCursorTargetTableName(); }
        else { tableNameNode = makeTableName( null, fromTable.getBaseTableName() ); }

        int            count = rcl.size();

        for ( int i = 0; i < count; i++ )
        {
            ResultColumn    column = (ResultColumn) rcl.elementAt( i );
            ColumnReference    reference = column.getReference();

            if ( (reference != null) && correlationName.equals( reference.getTableName() ) )
            {
                reference.setTableNameNode( tableNameNode );
            }
        }

    }

    /**
     * Check table name and then clear it from the result set columns.
     *
     * @exception StandardExcepion if invalid column/table is specified.
     */
    private void checkTableNameAndScrubResultColumns(ResultColumnList rcl, boolean isUpdateWithSubquery)
            throws StandardException
    {
        int columnCount = rcl.size();
        int tableCount = ((SelectNode)resultSet).fromList.size();

        for ( int i = 0; i < columnCount; i++ )
        {
            boolean foundMatchingTable = false;
            ResultColumn    column = (ResultColumn) rcl.elementAt( i );
            String columnTableName = column.getTableName();

            if (columnTableName != null) {
                for (int j = 0; j < tableCount; j++) {
                    FromTable fromTable = (FromTable) ((SelectNode)resultSet).
                            fromList.elementAt(j);
                    final String tableName;
                    final String exposedTableName;
                    if ( fromTable instanceof CurrentOfNode ) {
                        tableName = ((CurrentOfNode)fromTable).
                                getBaseCursorTargetTableName().getTableName();
                        exposedTableName = ((CurrentOfNode) fromTable).getExposedTableName().getTableName();

                    } else {
                        tableName = fromTable.getBaseTableName();
                        exposedTableName = fromTable.getExposedName();
                    }

                    if (columnTableName.equals(tableName) || columnTableName.equals(exposedTableName)) {
                        foundMatchingTable = true;
                        break;
                    }
                }

                if (!foundMatchingTable) {
                    throw StandardException.newException(
                            SQLState.LANG_COLUMN_NOT_FOUND,
                            column.getTableName() + "." + column.getName());
                }
            }


            if (!isUpdateWithSubquery) {
                /* The table name is
                 * unnecessary for an update.  More importantly, though, it
                 * creates a problem in the degenerate case with a positioned
                 * update.  The user must specify the base table name for a
                 * positioned update.  If a correlation name was specified for
                 * the cursor, then a match for the ColumnReference would not
                 * be found if we didn't null out the name.  (Aren't you
                 * glad you asked?)
                 */
                column.clearTableName();
            }
            else {
                /* Make the column reference's table name to target table's correlation name
                 which is consistent with getMatchingColumn in FromBaseTable
                */
                String crn = targetTable.correlationName;
                String baseTableName = targetTable.getBaseTableName();
                if (crn != null) {
                    ValueNode expression = column.getExpression();
                    if (!column.updated() && expression != null && expression instanceof ColumnReference && baseTableName.equals(expression.getTableName()))
                        ((ColumnReference) expression).setTableNameNode(makeTableName(null,crn));
                }
            }
        }
    }

    /**
     * Normalize synonym column references to have the name of the base table.
     *
     * @param rcl        The result column list of the target table
     * @param fromTable The table name to set the column refs to
     *
     * @exception StandardException        Thrown on error
     */
    private    void normalizeSynonymColumns(
    ResultColumnList    rcl, 
    FromTable           fromTable)
        throws StandardException
    {
        if (fromTable.getCorrelationName() != null)
        { 
            return; 
        }

        TableName tableNameNode;
        if (fromTable instanceof CurrentOfNode)
        {
            tableNameNode =
                ((CurrentOfNode) fromTable).getBaseCursorTargetTableName(); 
        }
        else
        {
            tableNameNode = makeTableName(null, fromTable.getBaseTableName());
        }

        super.normalizeSynonymColumns(rcl, tableNameNode);
    }
    
    /**
     * Do not allow generation clauses to be overriden. Throws an exception if
     * the user attempts to override the value of a generated column.  The only
     * value allowed in a generated column is DEFAULT. We will use
     * addedGeneratedColumns list to pass through the generated columns which
     * have already been added to the update list.
     *
     * @param targetRCL  the row in the table being UPDATEd
     * @param addedGeneratedColumns generated columns which the compiler added
     *        earlier on
     * @throws StandardException on error
     */
    private void forbidGenerationOverrides(
        ResultColumnList targetRCL,
        ColumnDescriptorList addedGeneratedColumns)
            throws StandardException
    {
        int  count = targetRCL.size();

        ResultColumnList    resultRCL = resultSet.getResultColumns();

        for ( int i = 0; i < count; i++ )
        {
            ResultColumn    rc = (ResultColumn) targetRCL.elementAt( i );

            if ( rc.hasGenerationClause() )
            {
                ValueNode   resultExpression =
                    ((ResultColumn) resultRCL.elementAt( i )).getExpression();

                if ( !( resultExpression instanceof DefaultNode) )
                {
                    //
                    // We may have added the generation clause
                    // ourselves. Here we forgive ourselves for this
                    // pro-active behavior.
                    //
                    boolean allIsForgiven = false;

                    String columnName =
                        rc.getTableColumnDescriptor().getColumnName();

                    int addedCount = addedGeneratedColumns.size();

                    for ( int j = 0; j < addedCount; j++ )
                    {
                        String addedColumnName = addedGeneratedColumns.
                            elementAt(j).getColumnName();

                        if ( columnName.equals( addedColumnName ) )
                        {
                            allIsForgiven = true;
                            break;
                        }
                    }
                    if ( allIsForgiven ) { continue; }

                    throw StandardException.newException
                        (SQLState.LANG_CANT_OVERRIDE_GENERATION_CLAUSE,
                         rc.getName() );
                }
                else
                {
                    // Skip this step if we're working on an update
                    // statement. For updates, the target list has already
                    // been enhanced.
                }
            }
        }
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
            .append("Update").append("(")
            .append("n=").append(getResultSetNode().getResultSetNumber()).append(attrDelim);
        if (this.resultSet!=null) {
            sb.append(this.resultSet.getFinalCostEstimate(false).prettyDmlStmtString("updatedRows"));
        }
        sb.append(attrDelim).append("targetTable=").append(targetTableName).append(")");
        return sb.toString();
    }

    @Override
    void verifyTargetTable() throws StandardException {
        super.verifyTargetTable();
        if(targetTable.getProperties()!=null) {
            Boolean pin = Boolean.parseBoolean(targetTable.getProperties().getProperty(PIN));
            if (pin) {
                throw StandardException.newException(SQLState.UPDATE_PIN_VIOLATION);
            }
        }

        if (targetTableDescriptor.getTableType() == TableDescriptor.EXTERNAL_TYPE)
            throw StandardException.newException(SQLState.EXTERNAL_TABLES_ARE_NOT_UPDATEABLE, targetTableName);
    }
} // end of UpdateNode
