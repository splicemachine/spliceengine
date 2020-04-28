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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableProperties;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.vti.DeferModification;

import java.lang.reflect.Modifier;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


/**
 * A DeleteNode represents a DELETE statement. It is the top-level node
 * for the statement.
 *
 * For positioned delete, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 */

public class DeleteNode extends DMLModStatementNode
{
    public static final String PIN = "pin";
    /* Column name for the RowLocation column in the ResultSet */
    // Splice fork: changed this to public, like it is in UpdateNode.
    public static final String COLUMNNAME = "###RowLocationToDelete";

    public static final String BULK_DELETE_DIRECTORY = "bulkDeleteDirectory";
    private DataSetProcessorType dataSetProcessorType = DataSetProcessorType.DEFAULT_CONTROL;

    /* Filled in by bind. */
    protected boolean          deferred;
    protected ExecRow          emptyHeapRow;
    protected FromTable        targetTable;
    protected FormatableBitSet readColsBitSet;

    private boolean cascadeDelete;
    private StatementNode[] dependentNodes;

    private Properties targetProperties;
    private String     bulkDeleteDirectory;
    private int[] colMap;
    /**
     * Initializer for a DeleteNode.
     *
     * @param targetTableName    The name of the table to delete from
     * @param queryExpression    The query expression that will generate
     *                the rows to delete from the given table
     */

    public void init(Object targetTableName,
                     Object queryExpression,
                     Object targetProperties)
    {
        super.init(queryExpression);
        this.targetTableName = (TableName) targetTableName;
        this.targetProperties = (Properties) targetProperties;
    }

    public String statementToString()
    {
        return "DELETE";
    }

    /**
     * Bind this DeleteNode.  This means looking up tables and columns and
     * getting their types, and figuring out the result types of all
     * expressions, as well as doing view resolution, permissions checking,
     * etc.
     * <p>
     * If any indexes need to be updated, we add all the columns in the
     * base table to the result column list, so that we can use the column
     * values as look-up keys for the index rows to be deleted.  Binding a
     * delete will also massage the tree so that the ResultSetNode has
     * column containing the RowLocation of the base row.
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void bindStatement() throws StandardException
    {
        // We just need select privilege on the where clause tables
        getCompilerContext().pushCurrentPrivType( Authorizer.SELECT_PRIV);
        try
        {
            FromList    fromList = (FromList) getNodeFactory().getNode(
                                    C_NodeTypes.FROM_LIST,
                                    getNodeFactory().doJoinOrderOptimization(),
                                    getContextManager());
            ResultColumn                rowLocationColumn = null;
            CurrentRowLocationNode        rowLocationNode;
            TableName                    cursorTargetTableName = null;
            CurrentOfNode               currentOfNode = null;

            DataDictionary dataDictionary = getDataDictionary();
            super.bindTables(dataDictionary);

            // wait to bind named target table until the underlying
            // cursor is bound, so that we can get it from the
            // cursor if this is a positioned delete.

            // for positioned delete, get the cursor's target table.
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(resultSet != null && resultSet instanceof SelectNode,
                "Delete must have a select result set");

            SelectNode sel;
            sel = (SelectNode)resultSet;
            targetTable = (FromTable) sel.fromList.elementAt(0);
            if (targetTable instanceof CurrentOfNode)
            {
                currentOfNode = (CurrentOfNode) targetTable;

                cursorTargetTableName = currentOfNode.getBaseCursorTargetTableName();
                // instead of an assert, we might say the cursor is not updatable.
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(cursorTargetTableName != null);
            }

            if (targetTable instanceof FromVTI)
            {
                targetVTI = (FromVTI) targetTable;
                targetVTI.setTarget();
            }
            else
            {
                // positioned delete can leave off the target table.
                // we get it from the cursor supplying the position.
                if (targetTableName == null)
                {
                    // verify we have current of
                    if (SanityManager.DEBUG)
                        SanityManager.ASSERT(cursorTargetTableName!=null);

                targetTableName = cursorTargetTableName;
                }
                // for positioned delete, we need to verify that
                // the named table is the same as the cursor's target (base table name).
                else if (cursorTargetTableName != null)
                {
                    // this match requires that the named table in the delete
                    // be the same as a base name in the cursor.
                    if ( !targetTableName.equals(cursorTargetTableName))
                    {
                        throw StandardException.newException(SQLState.LANG_CURSOR_DELETE_MISMATCH,
                            targetTableName,
                            currentOfNode.getCursorName());
                    }
                }
            }

            // descriptor must exist, tables already bound.
            verifyTargetTable();

            // Check the validity of the targetProperties, if they exist
            if (targetProperties != null)
                verifyTargetProperties(dataDictionary);

            /* Generate a select list for the ResultSetNode - CurrentRowLocation(). */
            if (SanityManager.DEBUG)
                SanityManager.ASSERT((resultSet.resultColumns == null),
                              "resultColumns is expected to be null until bind time");


            if (targetTable instanceof FromVTI)
            {
                getResultColumnList();
                resultColumnList = targetTable.getResultColumnsForList(null,
                                resultColumnList, null);

                /* Set the new result column list in the result set */
                resultSet.setResultColumns(resultColumnList);
                /* Bind the expressions before the ResultColumns are bound */
                super.bindExpressions();
            }
            else
            {
                /*
                ** Start off assuming no columns from the base table
                ** are needed in the rcl.
                */

                resultColumnList = new ResultColumnList();

                FromBaseTable fbt = getResultColumnList(resultColumnList);

                readColsBitSet = getReadMap(dataDictionary,
                                        targetTableDescriptor);

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
                }

                /* Set the new result column list in the result set */
                resultSet.setResultColumns(resultColumnList);
                /* Bind the expressions before the ResultColumns are bound */
                super.bindExpressions();
                /*
                ** Construct an empty heap row for use in our constant action.
                */
                emptyHeapRow = targetTableDescriptor.getEmptyExecRow();

                /* Generate the RowLocation column */
                ResultColumn rowIdColumn = targetTable.getRowIdColumn();
                if (rowIdColumn == null) {
                    rowLocationNode = (CurrentRowLocationNode) getNodeFactory().getNode(
                            C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
                            getContextManager());
                    rowIdColumn =
                            (ResultColumn) getNodeFactory().getNode(
                                    C_NodeTypes.RESULT_COLUMN,
                                    COLUMNNAME,
                                    rowLocationNode,
                                    getContextManager());

                    rowLocationNode.setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.REF_NAME),
                                    false		/* Not nullable */
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

                rowLocationColumn.bindResultColumnToExpression();

                /* Append to the ResultColumnList */
                resultColumnList.addResultColumn(rowLocationColumn);

                /* Force the added columns to take on the table's correlation name, if any */
                correlateAddedColumns( resultColumnList, targetTable );
            }



            /* Bind untyped nulls directly under the result columns */
            resultSet.getResultColumns().
                bindUntypedNullsToResultColumns(resultColumnList);

            if (! (targetTable instanceof FromVTI))
            {
                /* Bind the new ResultColumn */
                rowLocationColumn.bindResultColumnToExpression();

                bindConstraints(dataDictionary,
                            getNodeFactory(),
                            targetTableDescriptor,
                            null,
                            resultColumnList,
                            (int[]) null,
                            readColsBitSet,
                            false,
                            true);  /* we alway include triggers in core language */

                /* If the target table is also a source table, then
                 * the delete will have to be in deferred mode
                 * For deletes, this means that the target table appears in a
                 * subquery.  Also, self-referencing foreign key deletes
                  * are deferred.  And triggers cause the delete to be deferred.
                 */
                if (resultSet.subqueryReferencesTarget(
                                    targetTableDescriptor.getName(), true) ||
                    requiresDeferredProcessing())
                {
                    deferred = true;
                }
            }
            else
            {
                deferred = VTIDeferModPolicy.deferIt( DeferModification.DELETE_STATEMENT,
                                                  targetVTI,
                                                  null,
                                                  sel.getWhereClause());
            }
            sel = null; // done with sel

            /* Verify that all underlying ResultSets reclaimed their FromList */
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(fromList.size() == 0,
                    "fromList.size() is expected to be 0, not " +
                    fromList.size() +
                    " on return from RS.bindExpressions()");
            }

            //In case of cascade delete , create nodes for
            //the ref action  dependent tables and bind them.
            if(fkTableNames != null)
            {
                String currentTargetTableName = targetTableDescriptor.getSchemaName() +
                         "." + targetTableDescriptor.getName();

                if(!isDependentTable){
                    //graph node
                    graphHashTable = new Hashtable();
                }

                /*Check whether the current tatget is already been explored.
                 *If we are seeing the same table name which we binded earlier
                 *means we have cyclic references.
                 */
                if(!graphHashTable.containsKey(currentTargetTableName))
                {
                    cascadeDelete = true;
                    int noDependents = fkTableNames.length;
                    dependentNodes = new StatementNode[noDependents];
                    graphHashTable.put(currentTargetTableName, noDependents);
                    for(int i =0 ; i < noDependents ; i ++)
                    {
                        dependentNodes[i] = getDependentTableNode(fkTableNames[i],
                                                              fkRefActions[i],
                                                              fkColDescriptors[i]);
                        dependentNodes[i].bindStatement();
                    }
                }
            }
            else
            {
                //case where current dependent table does not have dependent tables
                if(isDependentTable)
                {
                    String currentTargetTableName = targetTableDescriptor.getSchemaName()
                             + "." + targetTableDescriptor.getName();
                    graphHashTable.put(currentTargetTableName, 0);

                }
            }
            if (isPrivilegeCollectionRequired())
            {
                getCompilerContext().pushCurrentPrivType( getPrivType());
                getCompilerContext().addRequiredTablePriv( targetTableDescriptor);
                getCompilerContext().popCurrentPrivType();
            }
        }
        finally
        {
            getCompilerContext().popCurrentPrivType();
        }
    } // end of bind

    private void verifyTargetProperties(DataDictionary dd)
            throws StandardException
    {
        bulkDeleteDirectory = targetProperties.getProperty(BULK_DELETE_DIRECTORY);
        if (bulkDeleteDirectory != null) {
            dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);

        }
    }

    int getPrivType()
    {
        return Authorizer.DELETE_PRIV;
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
        //If delete table is on a SESSION schema table, then return true.
        return resultSet.referencesSessionSchema();
    }

    /**
     * Compile constants that Execution will use
     *
     * @exception StandardException        Thrown on failure
     */
    public ConstantAction    makeConstantAction() throws StandardException
    {

        /* Different constant actions for base tables and updatable VTIs */
        if (targetTableDescriptor != null)
        {
            // Base table
            int lockMode = resultSet.updateTargetLockMode();
            long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
            TransactionController tc = getLanguageConnectionContext().getTransactionCompile();
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

            ResultDescription resultDescription = null;
            if(isDependentTable)
            {
                //triggers need the result description ,
                //dependent tables  don't have a source from generation time
                //to get the result description
                resultDescription = makeResultDescription();
            }


            return    getGenericConstantActionFactory().getDeleteConstantAction
                ( heapConglomId,
                  targetTableDescriptor.getTableType(),
                  tc.getStaticCompiledConglomInfo(heapConglomId),
                        pkColumns,
                  indicesToMaintain,
                  indexConglomerateNumbers,
                  indexSCOCIs,
                  emptyHeapRow,
                  deferred,
                  false,
                  targetTableDescriptor.getUUID(),
                  lockMode,
                  null, null, null, 0, null, null,
                  resultDescription,
                  getFKInfo(),
                  getTriggerInfo(),
                  (readColsBitSet == null) ? (FormatableBitSet)null : new FormatableBitSet(readColsBitSet),
                  getReadColMap(targetTableDescriptor.getNumberOfColumns(),readColsBitSet),
                  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
                   (readColsBitSet == null) ?
                      targetTableDescriptor.getNumberOfColumns() :
                      readColsBitSet.getNumBitsSet(),
                  (UUID) null,
                  resultSet.isOneRowResultSet(),
                  null);
        }
        else
        {
            /* Return constant action for VTI
             * NOTE: ConstantAction responsible for preserving instantiated
             * VTIs for in-memory queries and for only preserving VTIs
             * that implement Serializable for SPSs.
             */
            return    getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.DELETE_STATEMENT,
                        deferred);
        }
    }

    /**
     * Code generation for delete.
     * The generated code will contain:
     *        o  A static member for the (xxx)ResultSet with the RowLocations
     *        o  The static member will be assigned the appropriate ResultSet within
     *           the nested calls to get the ResultSets.  (The appropriate cast to the
     *           (xxx)ResultSet will be generated.)
     *        o  The CurrentRowLocation() in SelectNode's select list will generate
     *           a new method for returning the RowLocation as well as a call to
     *           that method which will be stuffed in the call to the
     *            ProjectRestrictResultSet.
     *      o In case of referential actions, this function generate an
     *        array of resultsets on its dependent tables.
     *
     * @param acb    The ActivationClassBuilder for the class being built
     * @param mb    The execute() method to be built
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

        acb.pushGetResultSetFactoryExpression(mb);
        acb.newRowLocationScanResultSetName();
        resultSet.generate(acb, mb); // arg 1

        String resultSetGetter;
        int argCount;
        String parentResultSetId;

        // Base table
        if (targetTableDescriptor != null)
        {
            /* Create the declaration for the scan ResultSet which generates the
             * RowLocations to be deleted.
              * Note that the field cannot be static because there
             * can be multiple activations of the same activation class,
             * and they can't share this field.  Only exprN fields can
             * be shared (or, more generally, read-only fields).
             * RESOLVE - Need to deal with the type of the field.
             */

            String type = ClassName.CursorResultSet;
            String name = acb.getRowLocationScanResultSetName();
            if (!acb.cb.existsField(type, name)) {
                acb.newFieldDeclaration(Modifier.PRIVATE, type, name);
            }

            if(cascadeDelete || isDependentTable)
            {
                resultSetGetter = "getDeleteCascadeResultSet";
                argCount = 4;
            }
            else
            {
                resultSetGetter = "getDeleteResultSet";
                argCount = 1;
            }

        } else {
            argCount = 1;
            resultSetGetter = "getDeleteVTIResultSet";
        }

        if(isDependentTable)
        {
            mb.push(acb.addItem(makeConstantAction()));

        }else
        {
            if(cascadeDelete)
            {
                mb.push(-1); //root table.
            }
        }

        String        resultSetArrayType = ClassName.ResultSet + "[]";
        if(cascadeDelete)
        {
            parentResultSetId = targetTableDescriptor.getSchemaName() +
                                   "." + targetTableDescriptor.getName();
            // Generate the code to build the array
            LocalField arrayField =
                acb.newFieldDeclaration(Modifier.PRIVATE, resultSetArrayType);
            mb.pushNewArray(ClassName.ResultSet, dependentNodes.length);  // new ResultSet[size]
            mb.setField(arrayField);
            for(int index=0 ; index <  dependentNodes.length ; index++)
            {

                dependentNodes[index].setRefActionInfo(fkIndexConglomNumbers[index],
                                                       fkColArrays[index],
                                                       parentResultSetId,
                                                       true);
                mb.getField(arrayField); // first arg (resultset array reference)
                /*beetle:5360 : if too many statements are added  to a  method,
                 *size of method can hit  65k limit, which will
                 *lead to the class format errors at load time.
                 *To avoid this problem, when number of statements added
                 *to a method is > 2048, remaing statements are added to  a new function
                 *and called from the function which created the function.
                 *See Beetle 5135 or 4293 for further details on this type of problem.
                */
                if(mb.statementNumHitLimit(10))
                {
                    MethodBuilder dmb = acb.newGeneratedFun(ClassName.ResultSet, Modifier.PRIVATE);
                    dependentNodes[index].generate(acb,dmb); //generates the resultset expression
                    dmb.methodReturn();
                    dmb.complete();
                    /* Generate the call to the new method */
                    mb.pushThis();
                    //second arg will be generated by this call
                    mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, dmb.getName(), ClassName.ResultSet, 0);
                }else
                {
                    dependentNodes[index].generate(acb,mb); //generates the resultset expression
                }

                mb.setArrayElement(index);
            }
            mb.getField(arrayField); // fourth argument - array reference
        }
        else
        {
            if(isDependentTable)
            {
                mb.pushNull(resultSetArrayType); //No dependent tables for this table
            }
        }


        if(cascadeDelete || isDependentTable)
        {
            parentResultSetId = targetTableDescriptor.getSchemaName() +
                                   "." + targetTableDescriptor.getName();
            mb.push(parentResultSetId);

        }

        mb.push((double)this.resultSet.getFinalCostEstimate(false).getEstimatedRowCount());
        mb.push(this.resultSet.getFinalCostEstimate(false).getEstimatedCost());
        mb.push(targetTableDescriptor.getVersion());
        if ("getDeleteResultSet".equals(resultSetGetter)) {
            mb.push(this.printExplainInformationForActivation());
            BaseJoinStrategy.pushNullableString(mb, bulkDeleteDirectory);
            if (colMap != null && colMap.length > 0) {
                mb.push(acb.addItem(colMap));
            } else {
                mb.push(-1);
            }
            argCount += 3;
        }
        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, resultSetGetter, ClassName.ResultSet, argCount+3);

        if(!isDependentTable && cascadeDelete)
        {
            int numResultSets = acb.getRowCount();
            if(numResultSets > 0)
            {
                //generate activation.raParentResultSets = new NoPutResultSet[size]
                MethodBuilder constructor = acb.getConstructor();
                constructor.pushThis();
                constructor.pushNewArray(ClassName.CursorResultSet, numResultSets);
                constructor.putField(ClassName.BaseActivation,
                                     "raParentResultSets",
                                     ClassName.CursorResultSet + "[]");
                constructor.endStatement();
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
        return StatementType.DELETE;
    }


    /**
      *    Gets the map of all columns which must be read out of the base table.
      * These are the columns needed to:
      *
      *        o    maintain indices
      *        o    maintain foreign keys
      *
      *    The returned map is a FormatableBitSet with 1 bit for each column in the
      * table plus an extra, unsued 0-bit. If a 1-based column id must
      * be read from the base table, then the corresponding 1-based bit
      * is turned ON in the returned FormatableBitSet.
      *
      *    @param    dd                the data dictionary to look in
      *    @param    baseTable        the base table descriptor
      *
      *    @return    a FormatableBitSet of columns to be read out of the base table
      *
      * @exception StandardException        Thrown on error
      */
    public    FormatableBitSet    getReadMap
    (
        DataDictionary        dd,
        TableDescriptor        baseTable
    )
        throws StandardException
    {
        boolean[]    needsDeferredProcessing = new boolean[1];
        needsDeferredProcessing[0] = requiresDeferredProcessing();

        Vector        conglomVector = new Vector();
        relevantTriggers = new GenericDescriptorList();

        FormatableBitSet columnMap = DeleteNode.getDeleteReadMap(baseTable,conglomVector, relevantTriggers, needsDeferredProcessing, bulkDeleteDirectory!=null);

        colMap = getColMap(columnMap);
        markAffectedIndexes( conglomVector );

        adjustDeferredFlag( needsDeferredProcessing[0] );

        return    columnMap;
    }

    int[] getColMap(FormatableBitSet columnMap) {
        int[] colMap = new int[columnMap.getNumBitsSet()];
        int index = 0;
        for (int i = 0; i < columnMap.getLength(); i++)
        {
            if (columnMap.isSet(i)) {
                colMap[index++] = i;
            }
        }
        return colMap;
    }
    /**
     * In case of referential actions, we require to perform
     * DML (UPDATE or DELETE) on the dependent tables.
     * Following function returns the DML Node for the dependent table.
     */
    private StatementNode getDependentTableNode(String tableName, int refAction,
                                                ColumnDescriptorList cdl) throws StandardException
    {
        StatementNode node=null;

        int index = tableName.indexOf('.');
        String schemaName = tableName.substring(0 , index);
        String tName = tableName.substring(index+1);
        if(refAction == StatementType.RA_CASCADE)
        {
            node = getEmptyDeleteNode(schemaName , tName);
            ((DeleteNode)node).isDependentTable = true;
            ((DeleteNode)node).graphHashTable = graphHashTable;
        }

        if(refAction == StatementType.RA_SETNULL)
        {
            node = getEmptyUpdateNode(schemaName , tName, cdl);
            ((UpdateNode)node).isDependentTable = true;
            ((UpdateNode)node).graphHashTable = graphHashTable;
        }

        return node;
    }


    private StatementNode getEmptyDeleteNode(String schemaName, String targetTableName)
        throws StandardException
    {

        ValueNode whereClause = null;

        TableName tableName = new TableName();
        tableName.init(schemaName , targetTableName);

        NodeFactory nodeFactory = getNodeFactory();
        FromList   fromList = (FromList) nodeFactory.getNode(C_NodeTypes.FROM_LIST, getContextManager());
        FromTable fromTable = (FromTable) nodeFactory.getNode(
                                                    C_NodeTypes.FROM_BASE_TABLE,
                                                    tableName,
                                                    null,
                                                    ReuseFactory.getInteger(FromBaseTable.DELETE),
                                                    null,
                                                    getContextManager());

        //we would like to use references index & table scan instead of
        //what optimizer says for the dependent table scan.
        Properties targetProperties = new FormatableProperties();
        targetProperties.put("index", "null");
        ((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);
        SelectNode resultSet = (SelectNode) nodeFactory.getNode(
                                                     C_NodeTypes.SELECT_NODE,
                                                     null,
                                                     null,   /* AGGREGATE list */
                                                     fromList, /* FROM list */
                                                     whereClause, /* WHERE clause */
                                                     null, /* GROUP BY list */
                                                     null, /* having clause */
                                                     null, /* windows */
                                                     getContextManager());

        return (StatementNode) nodeFactory.getNode(
                                                    C_NodeTypes.DELETE_NODE,
                                                    tableName,
                                                    resultSet,
                                                    getContextManager());

    }



    private StatementNode getEmptyUpdateNode(String schemaName, 
                                             String targetTableName,
                                             ColumnDescriptorList cdl)
        throws StandardException
    {

        ValueNode whereClause = null;

        TableName tableName = new TableName();
        tableName.init(schemaName , targetTableName);

        NodeFactory nodeFactory = getNodeFactory();
        FromList   fromList = (FromList) nodeFactory.getNode(C_NodeTypes.FROM_LIST, getContextManager());
        FromTable fromTable = (FromTable) nodeFactory.getNode(
                                                    C_NodeTypes.FROM_BASE_TABLE,
                                                    tableName,
                                                    null,
                                                    ReuseFactory.getInteger(FromBaseTable.DELETE),
                                                    null,
                                                    getContextManager());


        //we would like to use references index & table scan instead of
        //what optimizer says for the dependent table scan.
        Properties targetProperties = new FormatableProperties();
        targetProperties.put("index", "null");
        ((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);

        SelectNode resultSet = (SelectNode) nodeFactory.getNode(
                                                     C_NodeTypes.SELECT_NODE,
                                                     getSetClause(tableName, cdl),
                                                     null,   /* AGGREGATE list */
                                                     fromList, /* FROM list */
                                                     whereClause, /* WHERE clause */
                                                     null, /* GROUP BY list */
                                                     null, /* having clause */
                                                     null, /* windows */
                                                     getContextManager());

        return (StatementNode) nodeFactory.getNode(
                                                    C_NodeTypes.UPDATE_NODE,
                                                    tableName,
                                                    resultSet,
                                                    getContextManager());

    }


 
    private ResultColumnList getSetClause(TableName tabName,
                                          ColumnDescriptorList cdl)
        throws StandardException
    {
        ResultColumn resultColumn;
        ValueNode     valueNode;

        NodeFactory nodeFactory = getNodeFactory();
        ResultColumnList    columnList = (ResultColumnList) nodeFactory.getNode(
                                                C_NodeTypes.RESULT_COLUMN_LIST,
                                                getContextManager());

        valueNode =  (ValueNode) nodeFactory.getNode(C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE,
                                                             getContextManager());
        for(int index =0 ; index < cdl.size() ; index++)
        {
            ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
            //only columns that are nullable need to be set to 'null' for ON
            //DELETE SET NULL
            if((cd.getType()).isNullable())
            {
                resultColumn = (ResultColumn) nodeFactory.getNode(
                                           C_NodeTypes.RESULT_COLUMN,
                                        cd,
                                        valueNode,
                                        getContextManager());

                columnList.addResultColumn(resultColumn);
            }
        }
        return columnList;
    }


    public void optimizeStatement() throws StandardException
    {
        if(cascadeDelete)
        {
            for (StatementNode dependentNode : dependentNodes) {
                dependentNode.optimizeStatement();
            }
        }

        getCompilerContext().setDataSetProcessorType(dataSetProcessorType);
        super.optimizeStatement();
    }

    /**
      *    Builds a bitmap of all columns which should be read from the
      *    Store in order to satisfy an DELETE statement.
      *
      *
      *    1)    finds all indices on this table
      *    2)    adds the index columns to a bitmap of affected columns
      *    3)    adds the index descriptors to a list of conglomerate
      *        descriptors.
      *    4)    finds all DELETE triggers on the table
      *    5)    if there are any DELETE triggers, then do one of the following
      *     a)If all of the triggers have MISSING referencing clause, then that
      *      means that the trigger actions do not have access to before and
      *      after values. In that case, there is no need to blanketly decide
      *      to include all the columns in the read map just because there are
      *      triggers defined on the table.
      *     b)Since one/more triggers have REFERENCING clause on them, get all
      *      the columns because we don't know what the user will ultimately
      *      reference.
      *    6)    adds the triggers to an evolving list of triggers
      *
      *    @param    conglomVector        OUT: vector of affected indices
      *    @param    relevantTriggers    IN/OUT. Passed in as an empty list. Filled in as we go.
      *    @param    needsDeferredProcessing            IN/OUT. true if the statement already needs
      *                                            deferred processing. set while evaluating this
      *                                            routine if a trigger requires
      *                                            deferred processing
      *
      * @return a FormatableBitSet of columns to be read out of the base table
      *
      * @exception StandardException        Thrown on error
      */
    private static FormatableBitSet getDeleteReadMap
    (
        TableDescriptor                baseTable,
        Vector                        conglomVector,
        GenericDescriptorList        relevantTriggers,
        boolean[]                    needsDeferredProcessing,
        boolean                     isBulkDelete
    )
        throws StandardException
    {
        int        columnCount = baseTable.getMaxColumnID();
        FormatableBitSet    columnMap = new FormatableBitSet(columnCount + 1);

        /*
        ** Get a list of the indexes that need to be
        ** updated.  ColumnMap contains all indexed
        ** columns where 1 or more columns in the index
        ** are going to be modified.
        **
        ** Notice that we don't need to add constraint
        ** columns.  This is because we add all key constraints
        ** (e.g. foreign keys) as a side effect of adding their
        ** indexes above.  And we don't need to deal with
        ** check constraints on a delete.
        **
        ** Adding indexes also takes care of the replication
        ** requirement of having the primary key.
        */

        // Not Required for dynamic deletes.  It will be required if we attempt to do bulk
        // deletes from a file (JUN)

        if (isBulkDelete) {
            DMLModStatementNode.getXAffectedIndexes(baseTable, null, columnMap, conglomVector, true);
        }

        /*
         ** If we have any DELETE triggers, then do one of the following
         ** 1)If all of the triggers have MISSING referencing clause, then that
         ** means that the trigger actions do not have access to before and
         ** after values. In that case, there is no need to blanketly decide to
         ** include all the columns in the read map just because there are
         ** triggers defined on the table.
         ** 2)Since one/more triggers have REFERENCING clause on them, get all
         ** the columns because we don't know what the user will ultimately reference.
         */
        baseTable.getAllRelevantTriggers( StatementType.DELETE, (int[])null, relevantTriggers );

        if (relevantTriggers.size() > 0)
        {
            needsDeferredProcessing[0] = true;

            boolean needToIncludeAllColumns = false;
            for (Object relevantTrigger : relevantTriggers) {
                TriggerDescriptor trd = (TriggerDescriptor) relevantTrigger;
                //Does this trigger have REFERENCING clause defined on it.
                //If yes, then read all the columns from the trigger table.
                if (!trd.getReferencingNew() && !trd.getReferencingOld()) {
                } else {
                    needToIncludeAllColumns = true;
                    break;
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
    
    /*
     * Force column references (particularly those added by the compiler)
     * to use the correlation name on the base table, if any.
     */
    private    void    correlateAddedColumns( ResultColumnList rcl, FromTable fromTable )
        throws StandardException
    {
        String        correlationName = fromTable.getCorrelationName();

        if ( correlationName == null ) { return; }

        TableName    correlationNameNode = makeTableName( null, correlationName );
        int            count = rcl.size();

        for ( int i = 0; i < count; i++ )
        {
            ResultColumn    column = (ResultColumn) rcl.elementAt( i );

            ValueNode        expression = column.getExpression();

            if ( (expression != null) && (expression instanceof ColumnReference) )
            {
                ColumnReference    reference = (ColumnReference) expression;

                reference.setTableNameNode( correlationNameNode );
            }
        }

    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
            .append("Delete").append("(")
            .append("n=").append(getResultSetNode().getResultSetNumber()).append(attrDelim);
        if (this.resultSet!=null) {
            sb.append(this.resultSet.getFinalCostEstimate(false).prettyDmlStmtString("deletedRows"));
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
                throw StandardException.newException(SQLState.DELETE_PIN_VIOLATION);
            }
        }
        if (targetTableDescriptor.getTableType() == TableDescriptor.EXTERNAL_TYPE)
            throw StandardException.newException(SQLState.EXTERNAL_TABLES_ARE_NOT_UPDATEABLE, targetTableName);
    }


}
