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

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.FKInfo;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import org.apache.commons.lang3.ArrayUtils;

/**
 * A DMLStatement for a table modification: to wit, INSERT
 * UPDATE or DELETE.
 *
 */

abstract class DMLModStatementNode extends DMLStatementNode
{
//    protected DataDictionary    dataDictionary;
    protected FromVTI            targetVTI;
    protected TableName            targetTableName;
    protected ResultColumnList    resultColumnList;
    protected int                 lockMode;        // lock mode for the target table

    protected FKInfo[]            fkInfo;            // array of FKInfo structures
                                                // generated during bind
    protected TriggerInfo        triggerInfo;    // generated during bind
    public TableDescriptor        targetTableDescriptor;


    /* The indexes that could be affected by this statement */
    public    IndexRowGenerator[]         indicesToMaintain;
    public    long[]                        indexConglomerateNumbers;
    public    String[]                    indexNames;
    protected ConstraintDescriptorList relevantCdl;
    protected GenericDescriptorList relevantTriggers;

    // PRIVATE
    private boolean            requiresDeferredProcessing;
    private    int                statementType;
    private boolean            bound;
    private ValueNode        checkConstraints;

    /* Info required to perform referential actions */
    protected String[] fkTableNames; // referencing table names.
    protected int[] fkRefActions;    //type of referential actions
    protected ColumnDescriptorList[]  fkColDescriptors;
    protected long[] fkIndexConglomNumbers; //conglomerate number of the backing index
    protected  boolean isDependentTable;
    protected int[][] fkColArrays;
    protected Hashtable graphHashTable;
                          // Hash Table which maitains the querytreenode graph 
    protected TableName synonymTableName;
    /* Primary Key Column numbers */
    protected int[] pkColumns;


    /**
     * Initializer for a DMLModStatementNode -- delegate to DMLStatementNode
     *
     * @param resultSet    A ResultSetNode for the result set of the
     *            DML statement
     */
    public void init(Object resultSet)
    {
        super.init(resultSet);
        statementType = getStatementType();
    }

    /**
     * Initializer for a DMLModStatementNode -- delegate to DMLStatementNode
     *
     * @param resultSet    A ResultSetNode for the result set of the
     *            DML statement
     * @param statementType used by nodes that allocate a DMLMod directly
     *            (rather than inheriting it).
     */
    public void init(Object resultSet, Object statementType)
    {
        super.init(resultSet);
        this.statementType = (Integer) statementType;
    }

    void setTarget(QueryTreeNode targetName)
    {
        if (targetName instanceof TableName)
        {
            this.targetTableName = (TableName) targetName;
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                if (! (targetName instanceof FromVTI))
                {
                    SanityManager.THROWASSERT(
                        "targetName expected to be FromVTI, not " +
                        targetName.getClass().getName());
                }
            }
            this.targetVTI = (FromVTI) targetName;
            targetVTI.setTarget();
        }
    }

    /**
     * If the DML is on a temporary table, generate the code to mark temporary table as modified in the current UOW.
     * At rollback transaction (or savepoint), we will check if the temporary table was modified in that UOW.
     * If yes, we will remove all the data from the temporary table
     *
     * @param acb    The ActivationClassBuilder for the class being built
     * @param mb    The execute() method to be built
     *
     * @exception StandardException        Thrown on error
     */
    protected void generateCodeForTemporaryTable(ActivationClassBuilder acb, MethodBuilder mb)
        throws StandardException
    {
        if (targetTableDescriptor != null && targetTableDescriptor.getTableType() == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE &&
                targetTableDescriptor.isOnRollbackDeleteRows())
        {
            mb.pushThis();
            mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Activation,
                                    "getLanguageConnectionContext", ClassName.LanguageConnectionContext, 0);
            mb.push(targetTableDescriptor.getName());
            mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "markTempTableAsModifiedInUnitOfWork",
                        "void", 1);
            mb.endStatement();
        }
    }

    /**
     * Verify the target table.  Get the TableDescriptor
     * if the target table is not a VTI.
     *
     * @exception StandardException        Thrown on error
     */
    void verifyTargetTable()
        throws StandardException
    {
                DataDictionary dataDictionary = getDataDictionary();
        if (targetTableName != null)
        {
            /*
            ** Get the TableDescriptor for the table we are inserting into
            */
            SchemaDescriptor sdtc = getSchemaDescriptor(targetTableName.getSchemaName());

            targetTableDescriptor = getTableDescriptor(
                            targetTableName.getTableName(), sdtc);

            if (targetTableDescriptor == null)
            {
                // Check if the reference is for a synonym.
                TableName synonymTab = resolveTableToSynonym(targetTableName);
                if (synonymTab == null)
                    throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, targetTableName.toString());
                synonymTableName = targetTableName;
                targetTableName = synonymTab;
                sdtc = getSchemaDescriptor(targetTableName.getSchemaName());

                targetTableDescriptor = getTableDescriptor(synonymTab.getTableName(), sdtc);
                if (targetTableDescriptor == null)
                    throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, targetTableName.toString());
            }

            switch (targetTableDescriptor.getTableType())
            {
            case TableDescriptor.VIEW_TYPE:
                // Views are currently not updatable
                throw StandardException.newException(SQLState.LANG_VIEW_NOT_UPDATEABLE,
                        targetTableName);

            case TableDescriptor.VTI_TYPE:
                // fall through - currently all vti tables are system tables.
            case TableDescriptor.SYSTEM_TABLE_TYPE:
              // System tables are not updatable
                  throw StandardException.newException(SQLState.LANG_UPDATE_SYSTEM_TABLE_ATTEMPTED, targetTableName);
                default:
                break;
            }
            getCompilerContext().createDependency(targetTableDescriptor);
        }
        else
        {
            /* VTI - VTIs in DML Mod are version 2 VTIs - They
             * must implement java.sql.PreparedStatement and have
             * the JDBC2.0 getMetaData() and getResultSetConcurrency()
             * methods and return an updatable ResultSet.
             */
            FromList dummyFromList = new FromList();
            targetVTI = (FromVTI) targetVTI.bindNonVTITables(dataDictionary, dummyFromList);
            targetVTI = (FromVTI) targetVTI.bindVTITables(dummyFromList);
        }
    }

    /**
    * Get a schema descriptor for the given table.
    * Uses this.targetTableName.
    *
    * @return Schema Descriptor
    *
    * @exception    StandardException    throws on schema name
    *                        that doesn't exist
    */
    public SchemaDescriptor getSchemaDescriptor() throws StandardException
    {
        SchemaDescriptor        sd;

        sd = getSchemaDescriptor(targetTableName.getSchemaName());

        return sd;
    }

    /**
      Get a map to efficiently find heap columns from a compressed set of
      read columns. The returns a map such that

      <PRE>
      map[heapColId (0 based)] -> readCol id (0 based)
      </PRE>

      @param column_map_length The number of columns(ints) in the map.
      @param readColsBitSet A language style (1 based) bit set with bits for
      read heap columns set.

      RESOLVE: Replace this with a call to RowUtil when the store and
      the language both use 0 base or 1 base offsets for columns. Today
      we can't use the store function because we have a 1 based FormatableBitSet.
      */
    public static int[] getReadColMap(int column_map_length,FormatableBitSet readColsBitSet)
    {
        if (readColsBitSet == null) return null;

        int partial_col_cnt = 0;
        int column_map[] = new int[column_map_length];
        int readColsBitSetSize = readColsBitSet.size();

        for (int base_index = 0; base_index < column_map.length; base_index++)
        {
            if (readColsBitSetSize > base_index && readColsBitSet.get(base_index+1))
                column_map[base_index] = partial_col_cnt++;
            else
                // this column map offset entry should never be referenced.
                column_map[base_index] = -1;
        }

        return(column_map);
    }

    /**
     * Get and bind the ResultColumnList representing the columns in the
     * target table, given the table's name.
     *
     * @exception StandardException        Thrown on error
     */
    protected void getResultColumnList()
        throws StandardException
    {
        if (targetVTI == null)
        {
            getResultColumnList((ResultColumnList) null);
        }
        else
        {
            /* binding VTI - just point to VTI's RCL,
             * which was already bound.
             */
            resultColumnList = targetVTI.getResultColumns();
        }
    }

    /**
     * Get and bind the ResultColumnList representing the columns in the
     * target table, given the table's name.
     *
     * @exception StandardException        Thrown on error
     */
    protected FromBaseTable getResultColumnList(ResultColumnList    inputRcl)
        throws StandardException
    {
        /* Get a ResultColumnList representing all the columns in the target */
        FromBaseTable    fbt =
            (FromBaseTable)
                (getNodeFactory().getNode(
                                        C_NodeTypes.FROM_BASE_TABLE,
                                        synonymTableName != null ? synonymTableName : targetTableName,
                                        null,
                                        null,
                                        null,
                                        getContextManager())
                );

        fbt.bindNonVTITables(
            getDataDictionary(),
            (FromList) getNodeFactory().getNode(
                                C_NodeTypes.FROM_LIST,
                                getNodeFactory().doJoinOrderOptimization(),
                                getContextManager()));

        getResultColumnList(
                            fbt,
                            inputRcl
                            );
        return fbt;
    }

    /**
     * Get and bind the ResultColumnList representing the columns in the
     * target table, given a FromTable for the target table.
     *
     * @exception StandardException        Thrown on error
     */
    private void getResultColumnList(FromBaseTable    fromBaseTable,
                                        ResultColumnList    inputRcl)
        throws StandardException
    {
        if (inputRcl == null)
        {
            resultColumnList = fromBaseTable.getAllResultColumns(null);
            resultColumnList.bindResultColumnsByPosition(targetTableDescriptor);
        }
        else
        {
            resultColumnList = fromBaseTable.getResultColumnsForList(null, inputRcl,
                                                    fromBaseTable.getTableNameField());

            resultColumnList.bindResultColumnsByName(targetTableDescriptor,
                                                    (DMLStatementNode) this);
        }
    }

    /**
     * Parse and bind the generating expressions of computed columns.
     *
     * @param dataDictionary    metadata
     * @param targetTableDescriptor metadata for the table that has the generated columns
     * @param sourceRCL  the tuple stream which drives the INSERT or UPDATE
     * @param targetRCL  the row in the table that's being INSERTed or UPDATEd
     * @param forUpdate true if this is an UPDATE. false otherwise.
     * @param updateResultSet more information on the tuple stream driving the UPDATE
     */
    void parseAndBindGenerationClauses
    (
        DataDictionary        dataDictionary,
        TableDescriptor        targetTableDescriptor,
        ResultColumnList    sourceRCL,
        ResultColumnList    targetRCL,
        boolean                 forUpdate,
        ResultSetNode       updateResultSet
    )
        throws StandardException
    {
        CompilerContext             compilerContext = getCompilerContext();
        int  count = targetRCL.size();

        for ( int i = 0; i < count; i++ )
        {
            ResultColumn    rc = (ResultColumn) targetRCL.elementAt( i );

            //
            // For updates, there are two copies of the column in the row: a
            // before image and the actual value which will be set when we
            // update the row. We only want to compile a generation clause for
            // the value which will be updated.
            //
            if ( forUpdate && !rc.updated() ) { continue; }
            
            if ( rc.hasGenerationClause() )
            {
                ColumnDescriptor    colDesc = rc.getTableColumnDescriptor();
                DataTypeDescriptor  dtd = colDesc.getType();
                DefaultInfo             di = colDesc.getDefaultInfo();
                ValueNode   generationClause = parseGenerationClause( di.getDefaultText(), targetTableDescriptor );

                // insert CAST in case column data type is not same as the
                // resolved type of the generation clause
                generationClause = (ValueNode) getNodeFactory().getNode
                    (
                     C_NodeTypes.CAST_NODE,
                     generationClause,
                     dtd,
                     getContextManager()
                     );
                
                // Assignment semantics of implicit cast here:
                // Section 9.2 (Store assignment). There, General Rule 
                // 2.b.v.2 says that the database should raise an exception
                // if truncation occurs when stuffing a string value into a
                // VARCHAR, so make sure CAST doesn't issue warning only.
                ((CastNode)generationClause).setAssignmentSemantics();
                
                //
                // Unqualified function references should resolve to the
                // current schema at the time that the table was
                // created/altered. See DERBY-3945.
                //
                SchemaDescriptor    originalCurrentSchema = getSchemaDescriptor( di.getOriginalCurrentSchema(), true );
                compilerContext.pushCompilationSchema( originalCurrentSchema );

                try {
                    bindRowScopedExpression( getNodeFactory(), getContextManager(), targetTableDescriptor, sourceRCL, generationClause );
                }
                finally
                {
                    compilerContext.popCompilationSchema();
                }

                ResultColumn    newRC =  (ResultColumn) getNodeFactory().getNode
                    ( C_NodeTypes.RESULT_COLUMN, generationClause.getTypeServices(), generationClause, getContextManager());

                // replace the result column in place
                newRC.setVirtualColumnId( i + 1 ); // column ids are 1-based
                newRC.setColumnDescriptor( targetTableDescriptor, colDesc );
                targetRCL.setElementAt( newRC, i );

                // if this is an update, then the result column may appear in the
                // source list as well. replace it there too and perform a
                // little extra binding so that check constraints will bind and
                // generate correctly if they reference the generated column
                if ( forUpdate )
                {
                    for ( int j = 0; j < sourceRCL.size(); j++ )
                    {
                        if ( rc == sourceRCL.elementAt( j ) )
                        {
                            newRC.setName( rc.getName() );
                            newRC.setResultSetNumber( updateResultSet.getResultSetNumber() );
                            sourceRCL.setElementAt( newRC, j );
                            
                        }
                    }   // end of loop through sourceRCL
                }   // end if this is an update statement
            }  // end if this is a generated column
            
        }   // end of loop through targetRCL
    }
    
     /**
      *    Parse the generation clause for a column.
      *
      *    @param    clauseText  Text of the generation clause
      *
      * @return    The parsed expression as a query tree.
      *
      * @exception StandardException        Thrown on failure
      */
    public    ValueNode    parseGenerationClause
    (
     String                clauseText,
     TableDescriptor    td
    )
        throws StandardException
    {
        Parser                        p;
        ValueNode                    clauseTree;
        LanguageConnectionContext    lcc = getLanguageConnectionContext();

        /* Get a Statement to pass to the parser */

        /* We're all set up to parse. We have to build a compilable SQL statement
         * before we can parse -  So, we goober up a VALUES defaultText.
         */
        String select = "SELECT " + clauseText + " FROM " + td.getQualifiedName();

        /*
        ** Get a new compiler context, so the parsing of the select statement
        ** doesn't mess up anything in the current context (it could clobber
        ** the ParameterValueSet, for example).
        */
        CompilerContext newCC = lcc.pushCompilerContext();

        p = newCC.getParser();

        /* Finally, we can call the parser */
        // Since this is always nested inside another SQL statement, so topLevel flag
        // should be false
        Visitable qt = p.parseStatement(select);
        if (SanityManager.DEBUG)
        {
            if (! (qt instanceof CursorNode))
            {
                SanityManager.THROWASSERT(
                    "qt expected to be instanceof CursorNode, not " +
                    qt.getClass().getName());
            }
            CursorNode cn = (CursorNode) qt;
            if (! (cn.getResultSetNode() instanceof SelectNode))
            {
                SanityManager.THROWASSERT(
                    "cn.getResultSetNode() expected to be instanceof SelectNode, not " +
                    cn.getResultSetNode().getClass().getName());
            }
        }

        clauseTree = ((ResultColumn)
                            ((CursorNode) qt).getResultSetNode().getResultColumns().elementAt(0)).
                                    getExpression();

        lcc.popCompilerContext(newCC);

        return    clauseTree;
    }

    /**
     * Gets and binds all the constraints for an INSERT/UPDATE/DELETE.
     * First finds the constraints that are relevant to this node.
     * This is done by calling getAllRelevantConstriants().  If
     * getAllRelevantConstraints() has already been called, then
     * this list is used.  Then it creates appropriate
     * dependencies. Then binds check constraints.  It also
     * generates the array of FKInfo items that are used in
     * code generation.

     * Note: we have a new flag here to see if defer processing is enabled or
     *       not, the only scenario that is disabled is when we reapply the
     *         reply message we get from the source
     *
     *
     * @param dataDictionary        The DataDictionary
     * @param nodeFactory            Where to get query tree nodes.
     * @param targetTableDescriptor    The TableDescriptor
     * @param dependent            Parent object that will depend on all the constraints
     *                            that we look up. If this argument is null, then we
     *                            use the default dependent (the statement being compiled).
     * @param sourceRCL                RCL of the table being changed
     * @param changedColumnIds        If null, all columns being changed, otherwise array
     *                                of 1-based column ids for columns being changed
     * @param readColsBitSet        bit set for the read scan
     * @param skipCheckConstraints     whether to skip check constraints or not
     * @param includeTriggers        whether triggers are included in the processing
     *
     * @return    The bound, ANDed check constraints as a query tree.
     *
     * @exception StandardException        Thrown on failure
     */
    ValueNode bindConstraints
    (
        DataDictionary        dataDictionary,
        NodeFactory            nodeFactory,
        TableDescriptor        targetTableDescriptor,
        Dependent            dependent,
        ResultColumnList    sourceRCL,
        int[]                changedColumnIds,
        FormatableBitSet                readColsBitSet,
        boolean                skipCheckConstraints,
        boolean             includeTriggers
    )
        throws StandardException
    {
        bound = true;

        /* Nothing to do if updatable VTI */
        if (targetVTI != null)
        {
            return null;
        }

        CompilerContext compilerContext = getCompilerContext();
        
         // Donot need privileges to execute constraints
        compilerContext.pushCurrentPrivType( Authorizer.NULL_PRIV);
        try {
            getAllRelevantConstraints(dataDictionary,
                                            targetTableDescriptor,
                                            skipCheckConstraints,
                                            changedColumnIds);
            generatePKInfo(targetTableDescriptor);
            createConstraintDependencies(dataDictionary, relevantCdl, dependent);

        // Commented out the following line as part of work on DB-2229: referential actions.  The
        // referential actions we have implemented so far do not actually use Derby's FKInfo but
        // an assertion fails in this method when a DeleteNode is created for deleting by primary key.
        // We may need to re-enable this if future referential action implementations use FKInfo.
        //
        //generateFKInfo(relevantCdl, dataDictionary, targetTableDescriptor, readColsBitSet);

            getAllRelevantTriggers(dataDictionary, targetTableDescriptor,
                               changedColumnIds, includeTriggers);
            createTriggerDependencies(relevantTriggers, dependent);
            generateTriggerInfo(relevantTriggers, targetTableDescriptor, changedColumnIds);

            if (skipCheckConstraints)
            {
                return null;
            }

            checkConstraints = generateCheckTree(relevantCdl,
                                                        targetTableDescriptor);

            if (checkConstraints != null)
            {
                SchemaDescriptor    originalCurrentSchema = targetTableDescriptor.getSchemaDescriptor();
                compilerContext.pushCompilationSchema( originalCurrentSchema );

                try {
                    bindRowScopedExpression(nodeFactory, getContextManager(),
                                            targetTableDescriptor,
                                            sourceRCL,
                                            checkConstraints);
                }
                finally
                {
                    compilerContext.popCompilationSchema();
                }
            }
        }
        finally
        {
            compilerContext.popCurrentPrivType();
        }

        return    checkConstraints;
    }

    /**
     * Binds an already parsed expression that only involves columns in a single
     * row. E.g., a check constraint or a generation clause.
     *
     * @param nodeFactory            Where to get query tree nodes.
     * @param targetTableDescriptor    The TableDescriptor for the constrained table.
     * @param sourceRCL        Result columns.
     * @param expression        Parsed query tree for row scoped expression
     *
     * @exception StandardException        Thrown on failure
     */
    static void    bindRowScopedExpression
    (
        NodeFactory            nodeFactory,
        ContextManager    contextManager,
        TableDescriptor        targetTableDescriptor,
        ResultColumnList    sourceRCL,
        ValueNode            expression
    )
        throws StandardException
    {

        TableName    targetTableName = makeTableName
            (nodeFactory, contextManager, targetTableDescriptor.getSchemaName(), targetTableDescriptor.getName());

        /* We now have the expression as a query tree.  Now, we prepare
         * to bind that query tree to the source's RCL.  That way, the
         * generated code for the expression will be evaluated against the
         * source row to be inserted into the target table or
         * against the after portion of the source row for the update
         * into the target table.
         *        o  Goober up a new FromList which has a single table,
         *           a goobered up FromBaseTable for the target table
         *           which has the source's RCL as it RCL.
         *           (This allows the ColumnReferences in the expression
         *           tree to be bound to the right RCs.)
         *
          * Note that in some circumstances we may not actually verify
         * the expression against the source RCL but against a temp
         * row source used for deferred processing because of a trigger.
         * In this case, the caller of bindConstraints (UpdateNode)
         * has chosen to pass in the correct RCL to bind against.
         */
        FromList fakeFromList =
            (FromList) nodeFactory.getNode(
                            C_NodeTypes.FROM_LIST,
                            nodeFactory.doJoinOrderOptimization(),
                            contextManager);
        FromBaseTable table = (FromBaseTable)
            nodeFactory.getNode(
                C_NodeTypes.FROM_BASE_TABLE,
                targetTableName,
                null,
                sourceRCL,
                null,
                contextManager);
        table.setTableNumber(0);
        fakeFromList.addFromTable(table);

        // Now we can do the bind.
        expression.bindExpression(fakeFromList, null, null);
    }

    /**
     * Determine whether or not there are check constraints on the
     * specified table.
     *
     * @param dd    The DataDictionary to use
     * @param td    The TableDescriptor for the table
     *
     * @return Whether or not there are check constraints on the specified table.
     *
     * @exception StandardException        Thrown on failure
     */
    protected boolean hasCheckConstraints(DataDictionary dd,
                                          TableDescriptor td)
        throws StandardException
    {
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
        if (cdl == null)
            return false;
        ConstraintDescriptorList ccCDL = cdl.getSubList(DataDictionary.CHECK_CONSTRAINT);

        return (!ccCDL.isEmpty());
    }

    /**
     * Determine whether or not there are generated columns in the
     * specified table.
     *
     * @param td    The TableDescriptor for the table
     *
     * @return Whether or not there are generated columns in the specified table.
     *
     * @exception StandardException        Thrown on failure
     */
    protected boolean hasGenerationClauses(TableDescriptor td)
        throws StandardException
    {
        ColumnDescriptorList list= td.getGeneratedColumns();

        return (!list.isEmpty());
    }


    /**
     * Get the ANDing of all appropriate check constraints as 1 giant query tree.
     *
     * Makes the calling object (usually a Statement) dependent on all the constraints.
     *
     * @param cdl                The constriant descriptor list
     * @param td                The TableDescriptor
     *
     * @return    The ANDing of all appropriate check constraints as a query tree.
     *
     * @exception StandardException        Thrown on failure
     */
    private    ValueNode generateCheckTree
    (
        ConstraintDescriptorList    cdl,
        TableDescriptor                td
    )
        throws StandardException
    {
        ConstraintDescriptorList    ccCDL = cdl.getSubList(DataDictionary.CHECK_CONSTRAINT);
        int                            ccCDLSize = ccCDL.size();
        ValueNode                    checkTree = null;

        // Get the text of all the check constraints
        for (int index = 0; index < ccCDLSize; index++)
        {
            ConstraintDescriptor cd = ccCDL.elementAt(index);

            String constraintText = cd.getConstraintText();

            // Get the query tree for this constraint
            ValueNode oneConstraint =
                parseCheckConstraint(constraintText, td);

            // Put a TestConstraintNode above the constraint tree
            TestConstraintNode tcn =
                (TestConstraintNode) getNodeFactory().getNode(
                    C_NodeTypes.TEST_CONSTRAINT_NODE,
                    oneConstraint,
                    SQLState.LANG_CHECK_CONSTRAINT_VIOLATED,
                    td.getSchemaName()==null?td.getName():td.getSchemaName()+"."+td.getName(),
                    cd.getConstraintName(),
                    getContextManager());

            // Link consecutive TestConstraintNodes with AND nodes
            if (checkTree == null)
            {
                checkTree = tcn;
            }
            else
            {
                checkTree = (ValueNode) getNodeFactory().getNode(
                    C_NodeTypes.AND_NODE,
                    tcn,
                    checkTree,
                    getContextManager());
            }
        }

        return checkTree;
    }

    private void generatePKInfo(TableDescriptor td) throws StandardException {
        pkColumns = getPrimaryKeyInfo(td);
    }

    public static int[] getPrimaryKeyInfo(TableDescriptor td) throws StandardException {
        ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
        for (ConglomerateDescriptor cd : cdl) {
            if (cd.isPrimaryKey()) {
                return cd.getIndexDescriptor().baseColumnPositions();
            }
        }
        return null;
    }

    /**
     * Generate the TriggerInfo structures used during code generation.
     *
     * @param triggerList                The trigger descriptor list
     * @param td                The TableDescriptor
     * @param changedCols        The columns that are being modified
     *
     * @exception StandardException        Thrown on failure
     */
    private void generateTriggerInfo
    (
        GenericDescriptorList        triggerList,
        TableDescriptor                td,
        int[]                        changedCols
    )
        throws StandardException
    {
        if ((triggerList != null) && (!triggerList.isEmpty()))
        {
            triggerInfo = new TriggerInfo(td, changedCols, triggerList);
        }
    }

    /**
     * Return the FKInfo structure.  Just  a little wrapper
     * to make sure we don't try to access it until after
     * binding.
     *
     * @return the array of fkinfos
     */
    public FKInfo[] getFKInfo()
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(bound, "attempt to access FKInfo "+
                    "before binding");
        }
        return fkInfo;
    }

    /**
     * Return the TriggerInfo structure.  Just  a little wrapper
     * to make sure we don't try to access it until after
     * binding.
     *
     * @return the trigger info
     */
    public TriggerInfo getTriggerInfo()
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(bound, "attempt to access TriggerInfo "+
                    "before binding");
        }
        return triggerInfo;
    }

    /**
     * Get the check constraints for this node
     *
     * @return the check constraints, may be null
     */
    public ValueNode getCheckConstraints()
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(bound, "attempt to access FKInfo "+
                    "before binding");
        }
        return checkConstraints;
    }

    /**
     * Makes the calling object (usually a Statement) dependent on all the constraints.
     *
     * @param tdl                The trigger descriptor list
     * @param dependent            Parent object that will depend on all the constraints
     *                            that we look up. If this argument is null, then we
     *                            use the default dependent (the statement being compiled).
     *
     * @exception StandardException        Thrown on failure
     */
    private void createTriggerDependencies
    (
        GenericDescriptorList         tdl,
        Dependent                    dependent
    )
        throws StandardException
    {
        CompilerContext             compilerContext = getCompilerContext();

        for (Object aTdl : tdl) {
            TriggerDescriptor td = (TriggerDescriptor) aTdl;
            /*
            ** The dependent now depends on this trigger.
            ** The default dependent is the statement being compiled.
            */
            if (dependent == null) {
                compilerContext.createDependency(td);
            } else {
                compilerContext.createDependency(dependent, td);
            }
        }
    }

    /**
     * Get all the triggers relevant to this DML operation
     *
     * @param dd                The data dictionary
     * @param td                The TableDescriptor
     * @param changedColumnIds    If null, all columns being changed, otherwise array
     *                            of 1-based column ids for columns being changed
     * @param includeTriggers    whether we allow trigger processing or not for
     *                             this table
     *
     * @return    the constraint descriptor list
     *
     * @exception StandardException        Thrown on failure
     */
    protected GenericDescriptorList getAllRelevantTriggers
    (
        DataDictionary        dd,
        TableDescriptor        td,
        int[]                changedColumnIds,
        boolean             includeTriggers
    )
        throws StandardException
    {
        if ( relevantTriggers !=  null ) { return relevantTriggers; }

        relevantTriggers =  new GenericDescriptorList();

        if(!includeTriggers)
            return relevantTriggers;

        td.getAllRelevantTriggers( statementType, changedColumnIds, relevantTriggers );
        adjustDeferredFlag(!relevantTriggers.isEmpty());
        return relevantTriggers;
    }

    protected    void    adjustDeferredFlag( boolean adjustment )
    {
        if( !requiresDeferredProcessing ) { requiresDeferredProcessing = adjustment; }
    }

    /**
     * Get all of our dependents due to a constraint.
     *
     * Makes the calling object (usually a Statement) dependent on all the constraints.
     *
     * @param dd                The data dictionary
     * @param cdl                The constraint descriptor list
     * @param dependent            Parent object that will depend on all the constraints
     *                            that we look up. If this argument is null, then we
     *                            use the default dependent (the statement being compiled).
     *
     * @exception StandardException        Thrown on failure
     */
    private void createConstraintDependencies
    (
        DataDictionary                dd,
        ConstraintDescriptorList     cdl,
        Dependent                    dependent
    )
        throws StandardException
    {
        CompilerContext             compilerContext = getCompilerContext();

        int cdlSize = cdl.size();
        for (int index = 0; index < cdlSize; index++)
        {
            ConstraintDescriptor cd = cdl.elementAt(index);

            /*
            ** The dependent now depends on this constraint.
            ** the default dependent is the statement
            ** being compiled.
            */
            if (dependent == null)
            {
                compilerContext.createDependency(cd);
            }
            else
            {
                compilerContext.createDependency(dependent, cd);
            }

            /*
            ** We are also dependent on all referencing keys --
            ** if one of them is deleted, we'll have to recompile.
            ** Also, if there is a BULK_INSERT on the table
            ** we are going to scan to validate the constraint,
            ** the index number will change, so we'll add a
            ** dependency on all tables we will scan.
            */
            if (cd instanceof ReferencedKeyConstraintDescriptor)
            {
                ConstraintDescriptorList fkcdl = dd.getActiveConstraintDescriptors
                    ( ((ReferencedKeyConstraintDescriptor)cd).getForeignKeyConstraints(ConstraintDescriptor.ENABLED) );

                int fklSize = fkcdl.size();
                for (int inner = 0; inner < fklSize; inner++)
                {
                    ConstraintDescriptor fkcd = fkcdl.elementAt(inner);
                    if (dependent == null)
                    {
                        compilerContext.createDependency(fkcd);
                        compilerContext.createDependency(fkcd.getTableDescriptor());
                    }
                    else
                    {
                        compilerContext.createDependency(dependent, fkcd);
                        compilerContext.createDependency(dependent, fkcd.getTableDescriptor());
                    }
                }
            }
            else if (cd instanceof ForeignKeyConstraintDescriptor)
            {
                ForeignKeyConstraintDescriptor fkcd = (ForeignKeyConstraintDescriptor) cd;
                if (dependent == null)
                {
                    compilerContext.createDependency(fkcd.getReferencedConstraint().getTableDescriptor());
                }
                else
                {
                    compilerContext.createDependency(dependent,
                                    fkcd.getReferencedConstraint().getTableDescriptor());
                }
            }
        }
    }

    /**
     * Get all the constraints relevant to this DML operation
     *
     * @param dd                The DataDictionary
     * @param td                The TableDescriptor
     * @param skipCheckConstraints Skip check constraints
     * @param changedColumnIds    If null, all columns being changed, otherwise array
     *                            of 1-based column ids for columns being changed
     *
     * @return    the constraint descriptor list
     *
     * @exception StandardException        Thrown on failure
     */
    protected ConstraintDescriptorList getAllRelevantConstraints
    (
        DataDictionary        dd,
        TableDescriptor        td,
        boolean                skipCheckConstraints,
        int[]                changedColumnIds
    )
        throws StandardException
    {
        if ( relevantCdl != null ) { return relevantCdl; }

        boolean[]    needsDeferredProcessing = new boolean[1];
        relevantCdl = new ConstraintDescriptorList();

        needsDeferredProcessing[0] = requiresDeferredProcessing;
        td.getAllRelevantConstraints
            ( statementType, skipCheckConstraints, changedColumnIds,
              needsDeferredProcessing, relevantCdl );

        adjustDeferredFlag( needsDeferredProcessing[0] );

        return relevantCdl;
    }

    /**
     * Does this DML Node require deferred processing?
     * Set to true if we have triggers or referential
     * constraints that need deferred processing.
     *
     * @return true/false
     */
    public boolean requiresDeferredProcessing()
    {
        return requiresDeferredProcessing;
    }

    /**
      *    Parse a check constraint and turn it into a query tree.
      *
      *    @param    checkConstraintText    Text of CHECK CONSTRAINT.
      * @param    td                    The TableDescriptor for the table the the constraint is on.
      *
      *
      * @return    The parsed check constraint as a query tree.
      *
      * @exception StandardException        Thrown on failure
      */
    public    ValueNode    parseCheckConstraint
    (
        String                checkConstraintText,
        TableDescriptor        td
    )
        throws StandardException
    {
        Parser                        p;
        ValueNode                    checkTree;
        LanguageConnectionContext    lcc = getLanguageConnectionContext();

        /* Get a Statement to pass to the parser */

        /* We're all set up to parse. We have to build a compile SQL statement
         * before we can parse - we just have a WHERE clause right now.
         * So, we goober up a SELECT * FROM table WHERE checkDefs.
         */
        String select = "SELECT * FROM " +
                        td.getQualifiedName() +
                        " WHERE " +
                        checkConstraintText;

        /*
        ** Get a new compiler context, so the parsing of the select statement
        ** doesn't mess up anything in the current context (it could clobber
        ** the ParameterValueSet, for example).
        */
        CompilerContext newCC = lcc.pushCompilerContext();

        p = newCC.getParser();

        /* Finally, we can call the parser */
        // Since this is always nested inside another SQL statement, so topLevel flag
        // should be false
        Visitable qt = p.parseStatement(select);
        if (SanityManager.DEBUG)
        {
            if (! (qt instanceof CursorNode))
            {
                SanityManager.THROWASSERT(
                    "qt expected to be instanceof CursorNode, not " +
                    qt.getClass().getName());
            }
            CursorNode cn = (CursorNode) qt;
            if (! (cn.getResultSetNode() instanceof SelectNode))
            {
                SanityManager.THROWASSERT(
                    "cn.getResultSetNode() expected to be instanceof SelectNode, not " +
                    cn.getResultSetNode().getClass().getName());
            }
        }

        checkTree = ((SelectNode) ((CursorNode) qt).getResultSetNode()).getWhereClause();

        lcc.popCompilerContext(newCC);

        return    checkTree;
    }


    /**
      *    Generate the code to evaluate a tree of CHECK CONSTRAINTS.
      *
      *    @param    checkConstraints    Bound query tree of ANDed check constraints.
      *    @param    ecb                    Expression Class Builder
      *
      *
      *
      * @exception StandardException        Thrown on error
      */
    public    void    generateCheckConstraints
    (
        ValueNode                checkConstraints,
        ExpressionClassBuilder    ecb,
        MethodBuilder            mb
    )
                            throws StandardException
    {
        // for the check constraints, we generate an exprFun
        // that evaluates the expression of the clause
        // against the current row of the child's result.
        // if there are no check constraints, simply pass null
        // to optimize for run time performance.

           // generate the function and initializer:
           // Note: Boolean lets us return nulls (boolean would not)
           // private Boolean exprN()
           // {
           //   return <<checkConstraints.generate(ps)>>;
           // }
           // static Method exprN = method pointer to exprN;

        // if there is no check constraint, we just want to pass null.
        if (checkConstraints == null)
        {
               mb.pushNull(ClassName.GeneratedMethod);
        }
        else
        {
            MethodBuilder    userExprFun = generateCheckConstraints(checkConstraints, ecb);

               // check constraint is used in the final result set
            // as an access of the new static
               // field holding a reference to this new method.
               ecb.pushMethodReference(mb, userExprFun);
        }
    }

    /**
      *    Generate a method to evaluate a tree of CHECK CONSTRAINTS.
      *
      *    @param    checkConstraints    Bound query tree of ANDed check constraints.
      *    @param    ecb                    Expression Class Builder
      *
      *
      *
      * @exception StandardException        Thrown on error
      */
    public    MethodBuilder    generateCheckConstraints
    (
        ValueNode                checkConstraints,
        ExpressionClassBuilder    ecb
    )
        throws StandardException
    {
        // this sets up the method and the static field.
        // generates:
        //     java.lang.Object userExprFun { }
        MethodBuilder userExprFun = ecb.newUserExprFun();

        // check constraint knows it is returning its value;

        /* generates:
         *    return <checkExpress.generate(ecb)>;
         * and adds it to userExprFun
         */

        checkConstraints.generateExpression(ecb, userExprFun);
        userExprFun.methodReturn();

        // we are done modifying userExprFun, complete it.
        userExprFun.complete();

        return userExprFun;
    }

    /**
      *    Generate the code to evaluate all of the generation clauses. If there
      *    are generation clauses, this routine builds an Activation method which
      *    evaluates the generation clauses and fills in the computed columns.
      *
      * @param rcl  describes the row of expressions to be put into the bas table
      * @param resultSetNumber  index of base table into array of ResultSets
      * @param isUpdate true if this is for an UPDATE statement
      * @param ecb code generation state variable
      * @param mb the method being generated
      *
      * @exception StandardException        Thrown on error
      */
    public    void    generateGenerationClauses
    (
        ResultColumnList            rcl,
        int                                 resultSetNumber,
        boolean                         isUpdate,
        ExpressionClassBuilder    ecb,
        MethodBuilder            mb
    )
                            throws StandardException
    {
        ResultColumn rc;
        int size = rcl.size();
        boolean hasGenerationClauses = false;

        for (int index = 0; index < size; index++)
        {
            rc = (ResultColumn) rcl.elementAt(index);

            //
            // Generated columns should be populated after the base row because
            // the generation clauses may refer to base columns that have to be filled
            // in first.
            //
            if ( rc.hasGenerationClause() )
            {
                hasGenerationClauses = true;
                break;
            }
        }

        // we generate an exprFun
        // that evaluates the generation clauses
        // against the current row of the child's result.
        // if there are no generation clauses, simply pass null
        // to optimize for run time performance.

           // generate the function and initializer:
           // private Integer exprN()
           // {
        //   ...
           //   return 1 or NULL;
           // }
           // static Method exprN = method pointer to exprN;

        // if there are not generation clauses, we just want to pass null.
        if ( !hasGenerationClauses )
        {
               mb.pushNull(ClassName.GeneratedMethod);
        }
        else
        {
            MethodBuilder    userExprFun = generateGenerationClauses( rcl, resultSetNumber, isUpdate, ecb);

               // generation clause evaluation is used in the final result set
            // as an access of the new static
               // field holding a reference to this new method.
               ecb.pushMethodReference(mb, userExprFun);
        }
    }

    /**
      *    Generate a method to compute all of the generation clauses in a row.
      *
      * @param rcl  describes the row of expressions to be put into the bas table
      * @param rsNumber  index of base table into array of ResultSets
      * @param isUpdate true if this is for an UPDATE statement
      * @param ecb code generation state variable
      *
      */
    private    MethodBuilder    generateGenerationClauses
    (
        ResultColumnList            rcl,
        int                                 rsNumber,
        boolean                         isUpdate,
        ExpressionClassBuilder    ecb
    )
        throws StandardException
    {
        // this sets up the method and the static field.
        // generates:
        //     java.lang.Object userExprFun( ) { }
        MethodBuilder userExprFun = ecb.newUserExprFun();

        /* Push the the current row onto the stack. */
        userExprFun.pushThis();
        userExprFun.push( rsNumber );
        userExprFun.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getCurrentRow", ClassName.Row, 1);

        // Loop through the result columns, computing generated columns
        // as we go. 
        int     size = rcl.size();
        int     startColumn = 0;
        // For UPDATEs, we only compute the updated value for the
        // column. The updated value lives in the second half of the row.
        // This means we ignore the first half of the row, which holds
        // the before-images of the columns.
        if ( isUpdate )
        {
            // throw away the last cell in the row, which is the row id
            startColumn = size - 1;
            startColumn = startColumn / 2;
        }
        for ( int i = startColumn; i < size; i++ )
        {
            ResultColumn    rc = (ResultColumn) rcl.elementAt( i );

            if ( !rc.hasGenerationClause() ) { continue; }

            userExprFun.dup();       // instance (current row)
            userExprFun.push(i + 1); // arg1

            rc.generateExpression(ecb, userExprFun);
            userExprFun.cast(ClassName.DataValueDescriptor);
                
            userExprFun.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Row, "setColumn", "void", 2);
        }

        /* generates:
         *    return;
         * And adds it to userExprFun
         */
        userExprFun.methodReturn();

        // we are done modifying userExprFun, complete it.
        userExprFun.complete();

        return userExprFun;
    }

  /**
   * Generate an optimized QueryTree from a bound QueryTree.  Actually,
   * it can annotate the tree in place rather than generate a new tree,
   * but this interface allows the root node of the optimized QueryTree
   * to be different from the root node of the bound QueryTree.
   *
   * For non-optimizable statements, this method is a no-op.
   *
   * Throws an exception if the tree is not bound, or if the binding
   * is out of date.
   *
   *
   * @exception StandardException         Thrown on failure
   */
    public void optimizeStatement() throws StandardException
    {
        /* First optimize the query */
        super.optimizeStatement();

        /* In language we always set it to row lock, it's up to store to
         * upgrade it to table lock.  This makes sense for the default read
         * committed isolation level and update lock.  For more detail, see
         * Beetle 4133.
         */
        lockMode = TransactionController.MODE_RECORD;
    }

    /**
     * Get the list of indexes that must be updated by this DML statement.
     * WARNING: As a side effect, it creates dependencies on those indexes.
     *
     * @param td    The table descriptor for the table being updated
     * @param updatedColumns    The updated column list.  If not update, null
     * @param colBitSet            a 1 based bit set of the columns in the list
     *
     * @exception StandardException        Thrown on error
     */
    protected void getAffectedIndexes
    (
        TableDescriptor        td,
        ResultColumnList    updatedColumns,
        FormatableBitSet                colBitSet
    )
                    throws StandardException
    {
        Vector        conglomVector = new Vector();

        DMLModStatementNode.getXAffectedIndexes(td, updatedColumns, colBitSet, conglomVector, false);

        markAffectedIndexes( conglomVector );
    }
    /**
      *    Marks which indexes are affected by an UPDATE of the
      *    desired shape.
      *
      *    Is passed a list of updated columns. Does the following:
      *
      *    1)    finds all indices which overlap the updated columns
      *    2)    adds the index columns to a bitmap of affected columns
      *    3)    adds the index descriptors to a list of conglomerate
      *        descriptors.
      *
      *    @param    updatedColumns    a list of updated columns
      *    @param    colBitSet        OUT: evolving bitmap of affected columns
      *    @param    conglomVector    OUT: vector of affected indices
      *
      * @exception StandardException        Thrown on error
      */
    static void getXAffectedIndexes
    (
        TableDescriptor        baseTable,
        ResultColumnList    updatedColumns,
        FormatableBitSet    colBitSet,
        Vector                conglomVector,
        boolean             isBulkDelete
    )
        throws StandardException
    {
        ConglomerateDescriptor[]    cds = baseTable.getConglomerateDescriptors();

        /* we only get distinct conglomerate numbers.  If duplicate indexes
         * share one conglomerate, we only return one number.
         */
        long[] distinctConglomNums = new long[cds.length - 1];
        int distinctCount = 0;

        int[] primaryKeyColumns = getPrimaryKeyInfo(baseTable);

        for (ConglomerateDescriptor cd : cds) {
            if (!cd.isIndex()) {
                continue;
            }

            /*
            ** If this index doesn't contain any updated
            ** columns, then we can skip it.
            */
            if ((updatedColumns != null) &&
                    (!updatedColumns.updateOverlaps(
                            cd.getIndexDescriptor().baseColumnPositions()))) {
                continue;
            }

            if (conglomVector != null) {
                int i;
                for (i = 0; i < distinctCount; i++) {
                    if (distinctConglomNums[i] == cd.getConglomerateNumber())
                        break;
                }
                if (i == distinctCount)        // first appearence
                {
                    distinctConglomNums[distinctCount++] = cd.getConglomerateNumber();
                    conglomVector.add(cd);
                }
            }

            IndexRowGenerator ixd = cd.getIndexDescriptor();
            int[] cols = ixd.baseColumnPositions();


            if (colBitSet != null) {
                for (int col : cols) {
                    // Do not Add Primary Keys, we have them in the rowKey (BaseTable) or in the RowLocation (IndexTable)
                    if (!ArrayUtils.contains(primaryKeyColumns, col) || isBulkDelete)
                        colBitSet.set(col);
                }
            }    // end IF
        }        // end loop through conglomerates

    }

    protected    void    markAffectedIndexes
    (
        Vector    affectedConglomerates
    )
        throws StandardException
    {
        ConglomerateDescriptor    cd;
        int                        indexCount = affectedConglomerates.size();
        CompilerContext            cc = getCompilerContext();

        indicesToMaintain = new IndexRowGenerator[ indexCount ];
        indexConglomerateNumbers = new long[ indexCount ];
        indexNames = new String[indexCount];

        for ( int ictr = 0; ictr < indexCount; ictr++ )
        {
            cd = (ConglomerateDescriptor) affectedConglomerates.get( ictr );

            indicesToMaintain[ ictr ] = cd.getIndexDescriptor();
            indexConglomerateNumbers[ ictr ] = cd.getConglomerateNumber();
            indexNames[ictr] =
                ((cd.isConstraint()) ? null : cd.getConglomerateName());

            cc.createDependency(cd);
        }

    }


    public String statementToString()
    {
        return "DML MOD";
    }

    /**
     * Remap referenced columns in the cd to reflect the
     * passed in row map.
     *
     * @param cd         constraint descriptor
     * @param rowMap    1 based row map
     */
    private int[] remapReferencedColumns(ConstraintDescriptor cd, int[] rowMap)
    {
        int[] oldCols = cd.getReferencedColumns();
        if (rowMap == null)
        {
            return oldCols;
        }

        int[] newCols = new int[oldCols.length];
        for (int i = 0; i<oldCols.length; i++)
        {
            newCols[i] = rowMap[oldCols[i]];
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(newCols[i] != 0, "attempt to map a column "+
                    oldCols[i]+" which is not in our new column map.  Something is "+
                    "wrong with the logic to do partial reads for an update stmt");
            }
        }
        return newCols;
    }

    /**
     * Get a integer based row map from a bit set.
     *
     * @param bitSet
     * @param td
     *
     */
    private    int[] getRowMap(FormatableBitSet bitSet, TableDescriptor td)
        throws StandardException
    {
        if (bitSet == null)
        {
            return (int[])null;
        }

        int size = td.getMaxColumnID();
        int[] iArray = new int[size+1];
        int j = 1;
        for (int i = 1; i <= size; i++)
        {
            if (bitSet.get(i))
            {
                iArray[i] = j++;
            }
        }
        return iArray;
    }


    public void setRefActionInfo(long fkIndexConglomId,
                                 int[]fkColArray,
                                 String parentResultSetId,
                                 boolean dependentScan)
    {
        resultSet.setRefActionInfo(fkIndexConglomId,
                                   fkColArray,
                                   parentResultSetId,
                                   dependentScan);
    }

    /**
     * Normalize synonym column references to have the name of the base table.
     *
     * @param rcl               The result column list of the target table
     * @param targetTableName  The target tablename
     *
     * @exception StandardException        Thrown on error
     */
    public void normalizeSynonymColumns(
    ResultColumnList    rcl, 
    TableName           targetTableName)
        throws StandardException
    {
        if (synonymTableName == null)
            return;

        String synTableName = synonymTableName.getTableName();

        int    count = rcl.size();
        for (int i = 0; i < count; i++)
        {
            ResultColumn    column    = (ResultColumn) rcl.elementAt(i);
            ColumnReference    reference = column.getReference();

            if ( reference != null )
            {
                String crTableName = reference.getTableName();
                if ( crTableName != null )
                {
                    if ( synTableName.equals( crTableName ) )
                    {
                        reference.setTableNameNode( targetTableName );
                    }
                    else
                    {
                        throw StandardException.newException(
                                SQLState.LANG_TABLE_NAME_MISMATCH, 
                                synTableName, 
                                crTableName);
                    }
                }
            }
        }
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

            printLabel(depth, "targetTableName: ");
            targetTableName.treePrint(depth + 1);

            if (resultColumnList != null)
            {
                printLabel(depth, "resultColumnList: ");
                resultColumnList.treePrint(depth + 1);
            }
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (targetTableName != null)
        {
            targetTableName.accept(v, this);
        }
    }
}


