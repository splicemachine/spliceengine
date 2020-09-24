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
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.vti.DeferModification;

import java.sql.Types;
import java.util.List;
import java.util.Properties;

/**
 * An InsertNode is the top node in a query tree for an
 * insert statement.
 * <p>
 * After parsing, the node contains
 *   targetTableName: the target table for the insert
 *   collist: a list of column names, if specified
 *   queryexpr: the expression being inserted, either
 *                a values clause or a select form; both
 *                of these are represented via the SelectNode,
 *                potentially with a TableOperatorNode such as
 *                UnionNode above it.
 * <p>
 * After binding, the node has had the target table's
 * descriptor located and inserted, and the queryexpr
 * and collist have been massaged so that they are identical
 * to the table layout.  This involves adding any default
 * values for missing columns, and reordering the columns
 * to match the table's ordering of them.
 * <p>
 * After optimizing, ...
 */
public final class InsertNode extends DMLModStatementNode {
    public enum InsertMode {INSERT,UPSERT}

    public InsertMode insertMode = InsertMode.INSERT;

    public static final String INSERT_MODE = "insertMode";
    public static final String STATUS_DIRECTORY = "statusDirectory";
    public static final String BAD_RECORDS_ALLOWED = "badRecordsAllowed";
    public static final String USE_SPARK = "useSpark";
    public static final String USE_OLAP = "useOLAP";
    public static final String SKIP_CONFLICT_DETECTION = "skipConflictDetection";
    public static final String SKIP_WAL = "skipWAL";
    public static final String INSERT = "INSERT";
    public static final String PIN = "pin";
    public static final String BULK_IMPORT_DIRECTORY = "bulkImportDirectory";
    public static final String SAMPLING_ONLY = "samplingOnly";
    public static final String OUTPUT_KEYS_ONLY = "outputKeysOnly";
    public static final String SKIP_SAMPLING = "skipSampling";
    public static final String SAMPLE_FRACTION = "sampleFraction";
    public static final String INDEX_NAME = "index";

    public        ResultColumnList targetColumnList;
    public         boolean         deferred;
    public        ValueNode        checkConstraints;
    public        Properties       targetProperties;
    private     OrderByList        orderByList;
    private     ValueNode          offset;
    private     ValueNode          fetchFirst;
    private     boolean            hasJDBClimitClause; // true if using JDBC limit/offset escape syntax
    private     String             statusDirectory;
    private     boolean            skipConflictDetection = false;
    private     boolean            skipWAL = false;
    private     int                badRecordsAllowed = 0;
    private        String          bulkImportDirectory;
    private     boolean            samplingOnly;
    private     boolean            outputKeysOnly;
    private     boolean            skipSampling;
    private     double             sampleFraction;
    private     String             indexName;

    private DataSetProcessorType dataSetProcessorType = DataSetProcessorType.DEFAULT_CONTROL;


    protected   RowLocation[]         autoincRowLocation;
    /**
     * Initializer for an InsertNode.
     *
     * @param targetName    The name of the table/VTI to insert into
     * @param insertColumns    A ResultColumnList with the names of the
     *            columns to insert into.  May be null if the
     *            user did not specify the columns - in this
     *            case, the binding phase will have to figure
     *            it out.
     * @param queryExpression    The query expression that will generate
     *                the rows to insert into the given table
     * @param targetProperties    The properties specified on the target table
     * @param orderByList The order by list for the source result set, null if no order by list
     * @param offset The value of a <result offset clause> if present
     * @param fetchFirst The value of a <fetch first clause> if present
     * @param hasJDBClimitClause True if the offset/fetchFirst clauses come from JDBC limit/offset escape syntax
     */

    public void init(
            Object targetName,
            Object insertColumns,
            Object queryExpression,
            Object targetProperties,
            Object orderByList,
            Object offset,
            Object fetchFirst,
            Object hasJDBClimitClause)
    {
        /* statementType gets set in super() before we've validated
         * any properties, so we've kludged the code to get the
         * right statementType for a bulk insert replace.
         */
        super.init(
                queryExpression,
                ReuseFactory.getInteger(getStatementType(
                                                (Properties) targetProperties))
                );
        setTarget((QueryTreeNode) targetName);
        targetColumnList = (ResultColumnList) insertColumns;
        this.targetProperties = (Properties) targetProperties;
        this.orderByList = (OrderByList) orderByList;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        this.hasJDBClimitClause = hasJDBClimitClause != null && (Boolean) hasJDBClimitClause;

        /* Remember that the query expression is the source to an INSERT */
        getResultSetNode().setInsertSource();
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
            try {
                return ( (targetTableName!=null) ? targetTableName : targetVTI.getTableName() ).toString() + "\n"
                    + targetProperties + "\n"
                    + super.toString();
            } catch (com.splicemachine.db.iapi.error.StandardException e) {
                return "tableName: <not_known>\n"
                    + targetProperties + "\n"
                    + super.toString();
            }
        }
        else
        {
            return "";
        }
    }

    public String statementToString()
    {
        return "INSERT";
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

            if (targetColumnList != null)
            {
                printLabel(depth, "targetColumnList: ");
                targetColumnList.treePrint(depth + 1);
            }

            if (orderByList != null) {
                printLabel(depth, "orderByList: ");
                orderByList.treePrint(depth + 1);
            }

            /* RESOLVE - need to print out targetTableDescriptor */
        }
    }

    /**
     * Bind this InsertNode.  This means looking up tables and columns and
     * getting their types, and figuring out the result types of all
     * expressions, as well as doing view resolution, permissions checking,
     * etc.
     * <p>
     * Binding an insert will also massage the tree so that
     * the collist and select column order/number are the
     * same as the layout of the table in the store.
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void bindStatement() throws StandardException {
        // We just need select privilege on the expressions
        getCompilerContext().pushCurrentPrivType( Authorizer.SELECT_PRIV);
        FromList    fromList = (FromList) getNodeFactory().getNode(
                                    C_NodeTypes.FROM_LIST,
                                    getNodeFactory().doJoinOrderOptimization(),
                                    getContextManager());

        // Bind and Optimize Real Time Views (OK, That is a made up name).
        bindAndOptimizeRealTimeViews();

        /* If any underlying ResultSetNode is a SelectNode, then we
         * need to do a full bind(), including the expressions
         * (since the fromList may include a FromSubquery).
         */
        DataDictionary dataDictionary = getDataDictionary();
        super.bindResultSetsWithTables(dataDictionary);

        /*
        ** Get the TableDescriptor for the table we are inserting into
        */
        verifyTargetTable();

        // Check the validity of the targetProperties, if they exist
        if (targetProperties != null)
            verifyTargetProperties(dataDictionary);

        /*
        ** Get the resultColumnList representing the columns in the base
        ** table or VTI.
        */

        // this.resultColumnList is null at this point, but this invocation populates it
        // so that it can be utilized to transform this.resultSet.resultColumnList
        // farther down in this bindStatement logic. It is the RCL in this.resultSet
        // which is subsequently used in the activation.
        getResultColumnList();

        /* If we have a target column list, then it must have the same # of
         * entries as the result set's RCL.
         */
        if (targetColumnList != null) {
            /*
             * Normalize synonym qualifers for column references.
             */
            if (synonymTableName != null) {
                normalizeSynonymColumns ( targetColumnList, targetTableName );
            }

            /* Bind the target column list */
            getCompilerContext().pushCurrentPrivType( getPrivType());
            if (targetTableDescriptor != null) {
                targetColumnList.bindResultColumnsByName(targetTableDescriptor,
                                                        (DMLStatementNode) this);
            }
            else {
                targetColumnList.bindResultColumnsByName(targetVTI.getResultColumns(), targetVTI,
                                                        this);
            }
            getCompilerContext().popCurrentPrivType();
        }

        /* Verify that all underlying ResultSets reclaimed their FromList */
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(fromList.isEmpty(),
                "fromList.size() is expected to be 0, not " +
                fromList.size() +
                " on return from RS.bindExpressions()");
        }

        /* Replace any DEFAULTs with the associated tree, or flag DEFAULTs if
         * not allowed (inside top level set operator nodes). Subqueries are
         * checked for illegal DEFAULTs elsewhere.
         */
        boolean isTableConstructor =
            (resultSet instanceof UnionNode &&
             ((UnionNode)resultSet).tableConstructor()) ||
            resultSet instanceof RowResultSetNode;

        resultSet.replaceOrForbidDefaults(targetTableDescriptor,
                                          targetColumnList,
                                          isTableConstructor);

        /* Bind the expressions now that the result columns are bound
         * NOTE: This will be the 2nd time for those underlying ResultSets
         * that have tables (no harm done), but it is necessary for those
         * that do not have tables.  It's too hard/not work the effort to
         * avoid the redundancy.
         */
        List<SelectNode> selectNodeList = RSUtils.collectNodes(resultSet, SelectNode.class);
        // If underlying resultset is SelectNode, do not bind it again
        super.bindExpressions(selectNodeList.isEmpty());

        /*
        ** If the result set is a union, it could be a table constructor.
        ** Bind any nulls in the result columns of the table constructor
        ** to the types of the table being inserted into.
        **
        ** The types of ? parameters in row constructors and table constructors
        ** in an INSERT statement come from the result columns.
        **
        ** If there is a target column list, use that instead of the result
        ** columns for the whole table, since the columns in the result set
        ** correspond to the target column list.
        */
        if (targetColumnList != null) {
            if (resultSet.getResultColumns().visibleSize() > targetColumnList.size())
                throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED);
            resultSet.bindUntypedNullsToResultColumns(targetColumnList);
            resultSet.setTableConstructorTypes(targetColumnList);
        }
        else {
            if (resultSet.getResultColumns().visibleSize() > resultColumnList.size())
                throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED);
            resultSet.bindUntypedNullsToResultColumns(resultColumnList);
            resultSet.setTableConstructorTypes(resultColumnList);
        }

        /* Bind the columns of the result set to their expressions */
        resultSet.bindResultColumns(fromList);



        int resCols = resultSet.getResultColumns().visibleSize();
        DataDictionary dd = getDataDictionary();
        if (targetColumnList != null) {
            if (targetColumnList.size() != resCols)
                throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED);
        }
        else
        {
            if (targetTableDescriptor != null &&
                        targetTableDescriptor.getNumberOfColumns() != resCols)
                throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED);
        }

        /* See if the ResultSet's RCL needs to be ordered to match the target
         * list, or "enhanced" to accommodate defaults.  It can only need to
         * be ordered if there is a target column list.  It needs to be
         * enhanced if there are fewer source columns than there are columns
         * in the table.
         */
        boolean inOrder = true;
        int numTableColumns = resultColumnList.size();

        /* colMap[] will be the size of the target list, which could be larger
         * than the current size of the source list.  In that case, the source
         * list will be "enhanced" to include defaults.
         */

        // We can continue to use numTableColumns here, and to utilize column ordinal positions.
        // Taking storage positions into account for dropped column handling happens
        // at the very end of this bindStatement() logic.

        int[] colMap = new int[numTableColumns];

        // set the fields to an unused value
        for (int i = 0; i < colMap.length; i++)  {
            colMap[i] = -1;
        }

        /* Create the source/target list mapping */
        if (targetColumnList != null) {
            /*
            ** There is a target column list, so the result columns might
            ** need to be ordered.  Step through the target column list
            ** and remember the position in the target table of each column.
            ** Remember if any of the columns are out of order.
            */
            int targetSize = targetColumnList.size();
            for (int index = 0; index < targetSize; index++) {
                int position =
                    ((ResultColumn) (targetColumnList.elementAt(index))).
                                                columnDescriptor.getPosition();

                if (index != position-1) {
                    inOrder = false;
                }

                // position is 1-base; colMap indexes and entries are 0-based.
                colMap[position-1] = index;
            }
        }
        else {
            /*
            ** There is no target column list, so the result columns in the
            ** source are presumed to be in the same order as the target
            ** table.
            */
            for (int position = 0;
                position < resultSet.getResultColumns().visibleSize();
                position++) {
                colMap[position] = position;
            }
        }

        // Bind the ORDER BY columns
        if (orderByList != null) {
            orderByList.pullUpOrderByColumns(resultSet);

            // The select list may have new columns now, make sure to bind
            // those.

            super.bindTargetExpressions();

            orderByList.bindOrderByColumns(resultSet);
        }

        bindOffsetFetch(offset, fetchFirst);

        resultSet = enhanceAndCheckForAutoincrement(resultSet, inOrder, colMap);

        resultColumnList.checkStorableExpressions(resultSet.getResultColumns());

        // We need to expand both target (this.ResultColumnList) and source (this.ResultSet.resultColumnList)
        // to include null placeholders for dropped columns. We need each ResultColumn to have its virtualColumnId
        // be the storage position, not the ordinal column position. Ideally we would call this at the very bottom
        // of bindStatement() once everything is bound using the regular ordinal positions, but we need to do it
        // here because the normalize node is created here and it will end up wrong if we update result set later.
        expandResultSetWithDeletedColumns();

        /* Insert a NormalizeResultSetNode above the source if the source
         * and target column types and lengths do not match.
          */
        if (! resultColumnList.columnTypesAndLengthsMatch(
                                                resultSet.getResultColumns())) {
            
            resultSet =
            (ResultSetNode) getNodeFactory().getNode(
            C_NodeTypes.NORMALIZE_RESULT_SET_NODE, resultSet,
            resultColumnList, null, Boolean.FALSE,
            getContextManager());
        }

        if (targetTableDescriptor != null) {
            ResultColumnList sourceRCL = resultSet.getResultColumns();
            sourceRCL.copyResultColumnNames(resultColumnList);

            /* bind all generation clauses for generated columns */
            parseAndBindGenerationClauses
                ( dataDictionary, targetTableDescriptor, sourceRCL, resultColumnList, false, null );
            
            /* Get and bind all constraints on the table */
            checkConstraints = bindConstraints(dataDictionary,
                                                getNodeFactory(),
                                                targetTableDescriptor,
                                                null,
                                                sourceRCL,
                                                (int[]) null,
                                                (FormatableBitSet) null,
                                                false,
                                                true);  /* we always include
                                                         * triggers in core language */

            /* Do we need to do a deferred mode insert */
            /*
             ** Deferred if:
            **    If the target table is also a source table
            **    Self-referencing foreign key constraint
            **    trigger
            */
            if (resultSet.referencesTarget(
                                    targetTableDescriptor.getName(), true) ||
                 requiresDeferredProcessing()) {
                deferred = true;
            }

            /* Get the list of indexes on the table being inserted into */
            getAffectedIndexes(targetTableDescriptor);
            TransactionController tc =
                getLanguageConnectionContext().getTransactionCompile();

            autoincRowLocation =
                dd.computeAutoincRowLocations(tc, targetTableDescriptor);

            if (isPrivilegeCollectionRequired())
            {
                getCompilerContext().pushCurrentPrivType(getPrivType());
                getCompilerContext().addRequiredTablePriv(targetTableDescriptor);
                getCompilerContext().popCurrentPrivType();
            }

        }
        else
        {
            deferred = VTIDeferModPolicy.deferIt( DeferModification.INSERT_STATEMENT,
                                                  targetVTI,
                                                  null,
                                                  resultSet);
        }

        getCompilerContext().popCurrentPrivType();
    }

    /**
     * Updates this insert node's source and target result columns
     * (both this.resultSet.resultColumns and this.resultColumnList,
     * respectively) to include placeholder null columns for dropped columns.
     * This is accomplished by looking at column information within
     * this insert node's result column list (in particular the storage position
     * of each column) to determine where the additional null columns are needed.
     * Should generally only be called from bindStatement().
     *
     * @throws StandardException
     */
    protected void expandResultSetWithDeletedColumns() throws StandardException {
        // This logic applies only to InsertNode, not to UpdateNode,
        // which constructs its resultColumnList entirely differently.
        // That's why this method is here and not in the DMLModStatementNode.

        int maxColumnID = targetTableDescriptor.getMaxColumnID(); // 1-based
        int maxStorageID = targetTableDescriptor.getMaxStorageColumnID(); // 1-based
        if (maxColumnID == maxStorageID) {
            // No columns were dropped so no need to expand
            return;
        }

        int size = this.resultSet.getResultColumns().size();
        int[] storagePosMap = new int[maxStorageID];
        for (int i = 0; i < maxStorageID; i++)  {
            storagePosMap[i] = -1;
        }

        for (int index = 1; index <= size; index++) {
            ResultColumn targetRC = this.resultColumnList.getResultColumn(index);
            int storagePosition = targetRC.columnDescriptor.getStoragePosition();
            storagePosMap[storagePosition-1] = index;
        }

        ResultColumnList expandedRS = (ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST, getContextManager());
        ResultColumnList expandedRCL = (ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST, getContextManager());
        for (int index = 0; index < maxStorageID; index++) {
            int pos = storagePosMap[index]; // index is 0-based, pos is 1-based
            ResultColumn newSourceRC = null;
            ResultColumn newSourceRCLEntry = null;
            if (pos != -1) {
                ResultColumn sourceRC = this.resultSet.getResultColumns().getResultColumn(pos);
                newSourceRC = sourceRC.cloneMe();
                ResultColumn sourceRCLEntry = this.resultColumnList.getResultColumn(pos);
                newSourceRCLEntry = sourceRCLEntry.cloneMe();
            } else {
                // Have to give it a type, which doesn't need to match what the dropped column was.
                DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 1);
                // ValueNode nullNode = (ValueNode) getNodeFactory().getNode(
                //     C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE,
                //     getContextManager());
                newSourceRC = (ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "",
                    getNullNode(dtd),
                    getContextManager());
                newSourceRCLEntry = newSourceRC.cloneMe();
            }
            expandedRS.addResultColumn(newSourceRC);
            expandedRCL.addResultColumn(newSourceRCLEntry);
        }

        // In the new result columns, virtualColumnId = storage position id of the column
        resultSet.setResultColumns(expandedRS);
        this.resultColumnList = expandedRCL;
    }

    /**
     * Process ResultSet column lists for projection and autoincrement.
     *
     * This method recursively descends the result set node tree. When
     * it finds a simple result set, it processes any autoincrement
     * columns in that rs by calling checkAutoIncrement. When it finds
     * a compound result set, like a Union or a PRN, it recursively
     * descends to the child(ren) nodes. Union nodes can arise due to
     * multi-rows in VALUES clause), PRN nodes can arise when the set
     * of columns being inserted is a subset of the set of columns in
     * the table.
     *
     * In addition to checking for autoincrement columns in the result set,
     * we may need to enhance and re-order the column list to match the
     * column list of the table we are inserting into. This work is handled
     * by ResultsetNode.enhanceRCLForInsert.
     *
     * Note that, at the leaf level, we need to enhance the RCL first, then
     * check for autoincrement columns. At the non-leaf levels, we have
     * to enhance the RCL, but we don't have to check for autoincrement
     * columns, since they only occur at the leaf level.
     *
     * This way, all ColumnDescriptor of all rows will be set properly.
     *
     * @param resultSet            current node in the result set tree
     * @param inOrder            FALSE if the column list needs reordering
     * @param colMap            correspondence between RCLs
     * @return a node representing the source for the insert
     *
     * @exception StandardException Thrown on error
     */
    ResultSetNode enhanceAndCheckForAutoincrement(
            ResultSetNode resultSet, boolean inOrder, int[] colMap)
        throws StandardException
    {
        /*
         * Some implementation notes:
         *
         * colmap[x] == y means that column x in the target table
         * maps to column y in the source result set.
         * colmap[x] == -1 means that column x in the target table
         * maps to its default value.
         * both colmap indexes and values are 0-based.
         *
         * if the list is in order and complete, we don't have to change
         * the tree. If it is not, then we call RSN.enhanceRCLForInsert()
         * which will reorder ("enhance") the source RCL within the same RSN)
         *
         * one thing we do know is that all of the resultsets underneath
         * us have their resultColumn names filled in with the names of
         * the target table columns.  That makes generating the mapping
         * "easier" -- we simply generate the names of the target table columns
         * that are included.  For the missing columns, we generate default
         * value expressions.
         */

        resultSet = resultSet.enhanceRCLForInsert(this, inOrder, colMap);

        // Forbid overrides for generated columns and identity columns that
        // are defined as GENERATED ALWAYS.
        if ((resultSet instanceof UnionNode) &&
                ((UnionNode) resultSet).tableConstructor()) {
            // If this is a multi-row table constructor, we are not really
            // interested in the result column list of the top-level UnionNode.
            // The interesting RCLs are those of the RowResultSetNode children
            // of the UnionNode, and they have already been checked from
            // UnionNode.enhanceRCLForInsert(). Since the RCL of the UnionNode
            // doesn't tell whether or not DEFAULT is specified at the leaf
            // level, we need to skip it here to avoid false positives.
        } else {
            resultColumnList.forbidOverrides(resultSet.getResultColumns());
        }

        return resultSet;
    }

    int getPrivType()
    {
        return Authorizer.INSERT_PRIV;
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
        boolean returnValue = false;

        //If this node references a SESSION schema table, then return true.
        if (targetTableDescriptor != null)
            returnValue = isSessionSchema(targetTableDescriptor.getSchemaDescriptor());

        if (!returnValue)
            returnValue = resultSet.referencesSessionSchema();

        return returnValue;
    }

    /**
     * Verify that the target properties that we are interested in
     * all hold valid values.
     * NOTE: Any target property which is valid but cannot be supported
     * due to a target database, etc. will be turned off quietly.
     *
     * @param dd    The DataDictionary
     *
     * @exception StandardException        Thrown on error
     */
    private void verifyTargetProperties(DataDictionary dd)
        throws StandardException {
        // The only property that we're currently interested in is insertMode
        String insertModeString = targetProperties.getProperty(INSERT_MODE);
        String statusDirectoryString = targetProperties.getProperty(STATUS_DIRECTORY);
        bulkImportDirectory = targetProperties.getProperty(BULK_IMPORT_DIRECTORY);
        samplingOnly = Boolean.parseBoolean(targetProperties.getProperty(SAMPLING_ONLY));
        outputKeysOnly = Boolean.parseBoolean(targetProperties.getProperty(OUTPUT_KEYS_ONLY));
        skipSampling = Boolean.parseBoolean(targetProperties.getProperty(SKIP_SAMPLING));
        sampleFraction = targetProperties.getProperty(SAMPLE_FRACTION) != null ?
                Double.parseDouble(targetProperties.getProperty(SAMPLE_FRACTION)) : 0;
        indexName = targetProperties.getProperty(INDEX_NAME);
        String failBadRecordCountString = targetProperties.getProperty(BAD_RECORDS_ALLOWED);
        Boolean pin = Boolean.parseBoolean(targetProperties.getProperty(PIN));
        if(pin){
            throw StandardException.newException(SQLState.INSERT_PIN_VIOLATION);
        }
        String skipConflictDetectionString = targetProperties.getProperty(SKIP_CONFLICT_DETECTION);
        String skipWALString = targetProperties.getProperty(SKIP_WAL);

        if (insertModeString != null) {
            String upperValue = StringUtil.SQLToUpperCase(insertModeString);
            try {
                insertMode = InsertMode.valueOf(upperValue);
            } catch (Exception e) {
                throw StandardException.newException(SQLState.LANG_INVALID_INSERT_MODE,
                        insertMode,
                        targetTableName);
            }
        }

        if (failBadRecordCountString != null)
            badRecordsAllowed = getIntProperty(failBadRecordCountString, "bulkFetch");

        if (statusDirectoryString != null) {
            // validated for writing in ImportUtils.generateFileSystemPathForWrite()
            statusDirectory = statusDirectoryString;
        }
        if (skipConflictDetectionString != null) {
            skipConflictDetection = Boolean.parseBoolean(StringUtil.SQLToUpperCase(skipConflictDetectionString));
        }

        if (skipWALString != null) {
            skipWAL = Boolean.parseBoolean(StringUtil.SQLToUpperCase(skipWALString));
        }

        // check for property "useSpark" or "useOLAP"
        for( String propertyStr : new String[]{ USE_OLAP, USE_SPARK } )
        {
            String val = targetProperties.getProperty(propertyStr);
            if (val != null) {
                try {
                    dataSetProcessorType = dataSetProcessorType.combine(
                            Boolean.parseBoolean(StringUtil.SQLToUpperCase(val)) ?
                            DataSetProcessorType.QUERY_HINTED_SPARK :
                            DataSetProcessorType.QUERY_HINTED_CONTROL);
                } catch (Exception sparkE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FORCED_SPARK,
                            propertyStr, val);
                }
            }
        }
    }



    /**
     * Do the bind time checks to see if bulkInsert is allowed on
     * this table.  bulkInsert is disallowed at bind time for:
     *        o  target databases
     *        o  (tables with triggers?)
     * (It is disallowed at execution time if the table has at
     * least 1 row in it or if it is a deferred mode insert.)
     *
     * @param dd    The DataDictionary
     * @param mode    The insert mode
     *
     * @return Whether or not bulkInsert is allowed.
     *
     * @exception StandardException        Thrown on error
     */
    private boolean verifyBulkInsert(DataDictionary dd, String mode)
        throws StandardException
    {
        return true;
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

            long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
            TransactionController tc =
                getLanguageConnectionContext().getTransactionCompile();
            int numIndexes = (targetTableDescriptor != null) ?
                                indexConglomerateNumbers.length : 0;
            StaticCompiledOpenConglomInfo[] indexSCOCIs =
                new StaticCompiledOpenConglomInfo[numIndexes];

            for (int index = 0; index < numIndexes; index++) {
                indexSCOCIs[index] = tc.getStaticCompiledConglomInfo(indexConglomerateNumbers[index]);
            }

            return    getGenericConstantActionFactory().getInsertConstantAction
                ( targetTableDescriptor,
                  heapConglomId,
                  tc.getStaticCompiledConglomInfo(heapConglomId),
                  pkColumns,
                  indicesToMaintain,
                  indexConglomerateNumbers,
                  indexSCOCIs,
                  indexNames,
                  deferred,
                  false,
                  targetTableDescriptor.getUUID(),
                  lockMode,
                  null, null,
                  targetProperties,
                  getFKInfo(),
                  getTriggerInfo(),
                  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
                  getIndexedCols(),
                  (UUID) null,
                  null,
                  null,
                  resultSet.isOneRowResultSet(),
                  autoincRowLocation
                  );
        }
        else
        {
            /* Return constant action for VTI
             * NOTE: ConstantAction responsible for preserving instantiated
             * VTIs for in-memory queries and for only preserving VTIs
             * that implement Serializable for SPSs.
             */
            return    getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.INSERT_STATEMENT,
                        deferred);
        }
    }

    /**
     * Create a boolean[] to track the (0-based) columns which are indexed.
     *
     * @return A boolean[] to track the (0-based) columns which are indexed.
     *
     * @exception StandardException        Thrown on failure
     */
    public boolean[] getIndexedCols() throws StandardException
    {
        /* Create a boolean[] to track the (0-based) columns which are indexed */
        boolean[] indexedCols = new boolean[targetTableDescriptor.getNumberOfColumns()];
        for (IndexRowGenerator anIndicesToMaintain : indicesToMaintain) {
            int[] colIds = anIndicesToMaintain.getIndexDescriptor().baseColumnPositions();

            for (int colId : colIds) {
                indexedCols[colId - 1] = true;
            }
        }

        return indexedCols;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Remove any duplicate ORDER BY columns and push an ORDER BY if present
     * down to the source result set, before calling super.optimizeStatement.
     * </p>
     */

    public void optimizeStatement() throws StandardException
    {
        // Push the order by list down to the ResultSet
        if (orderByList != null)
        {
            // If we have more than 1 ORDERBY columns, we may be able to
            // remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
            if (orderByList.size() > 1)
            {
                orderByList.removeDupColumns();
            }

            resultSet.pushOrderByList(orderByList);

            orderByList = null;
        }

        resultSet.pushOffsetFetchFirst( offset, fetchFirst, hasJDBClimitClause );

        if (targetTableDescriptor != null && targetTableDescriptor.getStoredAs() != null) {
            dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);
        }
        getCompilerContext().setDataSetProcessorType(dataSetProcessorType);

        super.optimizeStatement();
        
        //
        // If the insert stream involves a table function, attempt the bulk-insert
        // optimization. See https://issues.apache.org/jira/browse/DERBY-4789
        // We perform this check after optimization because the table function may be
        // wrapped in a view, which is only expanded at optimization time.
        //
        HasTableFunctionVisitor tableFunctionVisitor = new HasTableFunctionVisitor();
        this.accept( tableFunctionVisitor );
        // DERBY-5614: See if the target is a global temporary table (GTT),
        // in which case we don't support bulk insert.
        if ( tableFunctionVisitor.hasNode() &&
                !isSessionSchema(targetTableDescriptor.getSchemaDescriptor())) {
            requestBulkInsert();
        }
    }

    /**
     * Request bulk insert optimization at run time.
     */
    private void requestBulkInsert()
    {
        if ( targetProperties == null ) { targetProperties = new Properties(); }

        // Set bulkInsert if insertMode not already set. For the import procedures,
        // the insertMode property may be set already
        String key = "insertMode";
        String value = "bulkInsert";

        if ( targetProperties.getProperty( key ) == null )
        { targetProperties.put( key, value ); }
    }

    /**
     * Code generation for insert
     * creates an expression for:
     *   ResultSetFactory.getInsertResultSet(resultSet.generate(ps), generationClausesResult, checkConstrainResult, this )
     *
     * @param acb    The ActivationClassBuilder for the class being built
     * @param mb the method  for the execute() method to be built
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
        generateParameterValueSet(acb);

        int partitionReferenceItem = -1;
        int[] partitionBy = targetTableDescriptor.getPartitionBy();
        if (partitionBy.length != 0)
            partitionReferenceItem=acb.addItem(new ReferencedColumnsDescriptorImpl(partitionBy));


        /*
        ** Generate the insert result set, giving it either the original
        ** source or the normalize result set, the constant action,
        ** and "this".
        */

        acb.pushGetResultSetFactoryExpression(mb);

        // arg 1
        resultSet.generate(acb, mb);

        // arg 2 generate code to evaluate generation clauses
        generateGenerationClauses( resultColumnList, resultSet.getResultSetNumber(), false, acb, mb );

        // arg 3 generate code to evaluate CHECK CONSTRAINTS
        generateCheckConstraints( checkConstraints, acb, mb );
        mb.push(insertMode.toString());
        if (statusDirectory==null)
            mb.pushNull("java.lang.String");
        else
            mb.push(statusDirectory);
        mb.push(badRecordsAllowed);
        mb.push(skipConflictDetection);
        mb.push(skipWAL);
        mb.push((double) this.resultSet.getFinalCostEstimate(false).getEstimatedRowCount());
        mb.push(this.resultSet.getFinalCostEstimate(false).getEstimatedCost());
        mb.push(targetTableDescriptor.getVersion());
        mb.push(this.printExplainInformationForActivation());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getDelimited());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getEscaped());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getLines());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getStoredAs());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getLocation());
        BaseJoinStrategy.pushNullableString(mb,targetTableDescriptor.getCompression());
        mb.push(partitionReferenceItem);
        BaseJoinStrategy.pushNullableString(mb,bulkImportDirectory);
        mb.push(samplingOnly);
        mb.push(outputKeysOnly);
        mb.push(skipSampling);
        mb.push(sampleFraction);
        BaseJoinStrategy.pushNullableString(mb, indexName);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getInsertResultSet", ClassName.ResultSet, 25);
    }

    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected final int getStatementType()
    {
        return StatementType.INSERT;
    }

    /**
     * Return the statement type, where it is dependent on
     * the targetProperties.  (insertMode = replace causes
     * statement type to be BULK_INSERT_REPLACE.
     *
     * @return the type of statement
     */
    static final int getStatementType(Properties targetProperties)
    {
        int retval = StatementType.INSERT;

        // The only property that we're currently interested in is insertMode
        String insertMode = (targetProperties == null) ? null : targetProperties.getProperty("insertMode");
        if (insertMode != null)
        {
            String upperValue = StringUtil.SQLToUpperCase(insertMode);
            if (upperValue.equals("REPLACE"))
            {
                retval = StatementType.BULK_INSERT_REPLACE;
            }
        }
        return retval;
    }

    /**
     * Get the list of indexes on the table being inserted into.  This
     * is used by INSERT.  This is an optimized version of what
     * UPDATE and DELETE use.
     *
     * @param td    TableDescriptor for the table being inserted into
     *                or deleted from
     *
     * @exception StandardException        Thrown on error
     */
    private void getAffectedIndexes
    (
        TableDescriptor     td
    )
                    throws StandardException
    {
        IndexLister    indexLister = td.getIndexLister( );

        indicesToMaintain = indexLister.getDistinctIndexRowGenerators();
        indexConglomerateNumbers = indexLister.getDistinctIndexConglomerateNumbers();
        indexNames = indexLister.getDistinctIndexNames();



        /* Add dependencies on all indexes in the list */
        ConglomerateDescriptor[]    cds = td.getConglomerateDescriptors();
        CompilerContext cc = getCompilerContext();

        for (ConglomerateDescriptor cd : cds) {
            cc.createDependency(cd);
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
        if (targetColumnList != null) {
            targetColumnList.accept(v, this);
        }
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
            .append("Insert").append("(")
            .append("n=").append(getResultSetNode().getResultSetNumber()).append(attrDelim);
        if (this.resultSet!=null) {
            sb.append(this.resultSet.getFinalCostEstimate(false).prettyDmlStmtString("insertedRows"));
        }
        sb.append(attrDelim).append("targetTable=").append(targetTableName).append(")");
        return sb.toString();
    }

}
