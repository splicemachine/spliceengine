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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.sql.compile.TableName;

import java.util.List;
import java.util.Properties;

/**
 * Factory for creating ConstantActions.
 *
 * <P>Implementation note: For most operations, the ResultSetFactory
 *    determines if the operation is allowed in a readonly/target database.
 *    Because we perform JAR add/drop/replace with a utility rather than
 *    using normal language processing we never get a result set for these
 *    operations. For this reason, the ConstantActionFactory rather than
 *    the ResultSetFactory checks if the these operations are allowed.
 *
 * See SpliceGenericConstantActionFactory in spliceengine repo
 */
public abstract class GenericConstantActionFactory {
    ///////////////////////////////////////////////////////////////////////
    //
    //    CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////
    public    GenericConstantActionFactory() {
    }

    ///////////////////////////////////////////////////////////////////////
    //
    //    CONSTANT ACTION MANUFACTORIES
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * Get ConstantAction for SET CONSTRAINTS statement.
     *
     *  @param cdl            the constraints to set, if null,
     *                        we'll go ahead and set them all
     *  @param enable        if true, turn them on, if false
     *                        disable them
     *  @param unconditionallyEnforce    Replication sets this to true at
     *                                    the end of REFRESH. This forces us
     *                                    to run the included foreign key constraints even
     *                                    if they're already marked ENABLED.
     *    @param ddlList        Replication list of actions to propagate,
     *                        null unless a replication source
     */
    public abstract ConstantAction getSetConstraintsConstantAction(
        ConstraintDescriptorList    cdl,
        boolean                        enable,
        boolean                        unconditionallyEnforce,
        Object[]                    ddlList
    );


    /**
     *    Make the AlterAction for an ALTER TABLE statement.
     *
     *  @param sd            descriptor for the schema that table lives in.
     *  @param tableName    Name of table.
     *    @param tableId        UUID of table.
     *    @param tableConglomerateId    heap conglomerate id of table
     *  @param tableType    Type of table (e.g., BASE).
     *  @param columnInfo    Information on all the columns in the table.
     *  @param constraintActions    ConstraintConstantAction[] for constraints
     * @param lockGranularity    The lock granularity.
     *    @param compressTable    Whether or not this is a compress table
     *    @param behavior            drop behavior of dropping column
     *    @param sequential    If compress table/drop column, whether or not sequential
     *  @param truncateTable        Whether or not this is a truncate table
     *  @param purge                PURGE during INPLACE COMPRESS?
     *  @param defragment            DEFRAGMENT during INPLACE COMPRESS?
     *  @param truncateEndOfTable    TRUNCATE END during INPLACE COMPRESS?
     *  @param updateStatistics        TRUE means we are here to update statistics
     *  @param updateStatisticsAll    TRUE means we are here to update statistics
     *      of all the indexes. False means we are here to update statistics of
     *      only one index.
     *  @param dropStatistics        TRUE means we are here to drop statistics
     *  @param dropStatisticsAll    TRUE means we are here to drop statistics
     *      of all the indexes. False means we are here to drop statistics of
     *      only one index.
     *  @param indexNameForStatistics    Will name the index whose statistics
     *      will be updated/dropped. This param is looked at only if
     *      updateStatisticsAll/dropStatisticsAll is set to false and
     *      updateStatistics/dropStatistics is set to true.
     *  .
     */
    public abstract ConstantAction    getAlterTableConstantAction(
        SchemaDescriptor            sd,
        String                        tableName,
        UUID                        tableId,
        long                        tableConglomerateId,
        int                            tableType,
        ColumnInfo[]                columnInfo,
        ConstantAction[]     constraintActions,
        char                        lockGranularity,
        boolean                        compressTable,
        int                            behavior,
        boolean                        sequential,
        boolean                     truncateTable,
        boolean                        purge,
        boolean                        defragment,
        boolean                        truncateEndOfTable,
        boolean                        updateStatistics,
        boolean                        updateStatisticsAll,
        boolean                        dropStatistics,
        boolean                        dropStatisticsAll,
        String                        indexNameForStatistics
    );

    /**
     *    Make a ConstantAction for a constraint.
     *
     *  @param constraintName    Constraint name.
     *  @param constraintType    Constraint type.
     *  @param forCreateTable    True if for a CREATE TABLE
     *  @param tableName         Table name.
     *  @param tableId           UUID of table.
     *  @param schemaName        Schema that table lives in.
     *  @param columnNames       String[] for column names
     *  @param indexAction       IndexConstantAction for constraint (if necessary)
     *  @param constraintText    Text for check constraint
     *  @param enabled           Should the constraint be created as enabled
     *                            (enabled == true), or disabled (enabled == false).
     *  @param otherConstraint   The referenced constraint, if a foreign key constraint
     *  @param providerInfo      Information on all the Providers
     */
    public abstract ConstantAction getCreateConstraintConstantAction(
        String         constraintName,
        int            constraintType,
        boolean        forCreateTable,
        String         tableName,
        UUID           tableId,
        String         schemaName,
        String[]       columnNames,
        ConstantAction indexAction,
        String         constraintText,
        boolean        enabled,
        ConstraintInfo otherConstraint,
        ProviderInfo[] providerInfo
    );


    /**
     *     Make the ConstantAction for a CREATE INDEX statement.
     * 
     * @param forCreateTable Executed as part of a CREATE TABLE
     * @param unique            True means it will be a unique index
     * @param uniqueWithDuplicateNulls  True means index check and disallow
     *                                  any duplicate key if key has no 
     *                                  column with a null value.  If any 
     *                                  column in the key has a null value,
     *                                  no checking is done and insert will
     *                                  always succeed.
     * @param indexType         The type of index (BTREE, for example)
     * @param schemaName        The schema that table (and index) lives in.
     * @param indexName         Name of the index
     * @param tableName         Name of table the index will be on
     * @param tableId           UUID of table.
     * @param columnNames       Names of the columns in the index, in order
     * @param isAscending       Array of booleans telling asc/desc on each column
     * @param isConstraint      TRUE if index is backing up a constraint, else FALSE
     * @param conglomerateUUID  ID of conglomerate
     * @param properties        The optional properties list associated with the index.
     */
    public abstract ConstantAction getCreateIndexConstantAction
    (
        boolean              forCreateTable,
        boolean              unique,
        boolean              uniqueWithDuplicateNulls,
        String               indexType,
        String               schemaName,
        String               indexName,
        String               tableName,
        UUID                 tableId,
        String[]             columnNames,
        DataTypeDescriptor[] indexColumnTypes,
        boolean[]            isAscending,
        boolean              isConstraint,
        UUID                 conglomerateUUID,
        boolean              excludeNulls,
        boolean              excludeDefaults,
        boolean              preSplit,
        boolean              isLogicalKey,
        boolean              sampling,
        double               sampleFraction,
        String               splitKeyPath,
        String               hfilePath,
        String               columnDelimiter,
        String               characterDelimiter,
        String               timestampFormat,
        String               dateFormat,
        String               timeFormat,
        String[]             exprTexts,
        ByteArray[]          exprBytecode,
        String[]             generatedClassNames,
        Properties           properties
    );


    /**
     *    Make the ConstantAction for a CREATE ALIAS statement.
     *
     *  @param aliasName        Name of alias.
     *  @param schemaName        Alias's schema.
     *  @param javaClassName    Name of java class.
     *  @param aliasType        The alias type
     */
    public abstract ConstantAction    getCreateAliasConstantAction
    (
        String    aliasName,
        String    schemaName,
        String    javaClassName,
        AliasInfo    aliasInfo,
        char    aliasType);

    /**
     * Make the ConstantAction for a CREATE SCHEMA statement.
     *
     *  @param schemaName    Name of table.
     *  @param aid            Authorizaton id
     */
    public abstract ConstantAction    getCreateSchemaConstantAction
    (
        String            schemaName,
        String            aid);


    /**
     * Make the ConstantAction for a CREATE ROLE statement.
     *
     * @param roleName    Name of role.
     */
    public abstract ConstantAction    getCreateRoleConstantAction(String roleName);


    /**
     * Make the ConstantAction for a SET ROLE statement.
     *
     * @param roleName  Name of role
     * @param type      Literal (== 0)
     *                  or ?    (== StatementType.SET_ROLE_DYNAMIC)
     */
    public abstract ConstantAction getSetRoleConstantAction(String roleName,
                                                   int type);

    /**
     * Make the ConstantAction for a CREATE SEQUENCE statement.
     *
     * @param sequenceName    Name of sequence.
     */
    public abstract ConstantAction    getCreateSequenceConstantAction
    (
            TableName sequenceName,
            DataTypeDescriptor dataType,
            long        initialValue,
            long        stepValue,
            long        maxValue,
            long        minValue,
            boolean     cycle
    );

    /**
     *    Make the ConstantAction for a CREATE TABLE statement.
     *
     *  @param schemaName    name for the schema that table lives in.
     *  @param tableName    Name of table.
     *  @param tableType    Type of table (e.g., BASE, global temporary table).
     *  @param columnInfo    Information on all the columns in the table.
     *         (REMIND tableDescriptor ignored)
     *  @param constraintActions    CreateConstraintConstantAction[] for constraints
     *  @param properties    Optional table properties
     * @param createBehavior  CREATE_IF_NOT_EXISTS or CREATE_DEFAULT
     * @param lockGranularity    The lock granularity.
     * @param onCommitDeleteRows    If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows    If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     * @param withDataQueryString if not null, a statement that needs to execute to insert data into the table once fully created. If null, then nothing to be inserted
         *                 If not null, this MUST be bound and optimized AFTER the table has been created, or else it will break
     */
    public abstract    ConstantAction    getCreateTableConstantAction
    (
        String            schemaName,
        String            tableName,
        int                tableType,
        ColumnInfo[]    columnInfo,
        ConstantAction[] constraintActions,
        Properties        properties,
        int             createBehavior,
        char            lockGranularity,
        boolean            onCommitDeleteRows,
        boolean            onRollbackDeleteRows,
        String          withDataQueryString,
        boolean isExternal,
        String delimited,
        String escaped,
        String lines,
        String storedAs,
        String location,
        String compression,
        boolean mergeSchema,
        boolean presplit,
        boolean isLogicalKey,
        String splitKeyPath,
        String columnDelimiter,
        String characterDelimiter,
        String timestampFormat,
        String dateFormat,
        String timeFormat
        );

    public abstract    ConstantAction    getPinTableConstantAction (
                    String            schemaName,
                    String            tableName
            );


    /**
     *    Make the ConstantAction for a savepoint statement (ROLLBACK savepoint, RELASE savepoint and SAVEPOINT).
     *
     *  @param savepointName    name for the savepoint.
     *  @param statementType    Type of savepoint statement ie rollback, release or set savepoint
     */
    public abstract ConstantAction    getSavepointConstantAction (
        String            savepointName,
        int                statementType);


    /**
     *    Make the ConstantAction for a CREATE VIEW statement.
     *
     *  @param schemaName    Name of the schema that table lives in.
     *  @param tableName    Name of table.
     *  @param tableType    Type of table (in this case TableDescriptor.VIEW_TYPE).
     *    @param viewText        Text of query expression for view definition
     *  @param checkOption    Check option type
     *  @param columnInfo    Information on all the columns in the table.
     *  @param providerInfo Information on all the Providers
     *    @param compSchemaId    ID of schema in which the view is to be bound
     *                        when accessed in the future.
     *         (REMIND tableDescriptor ignored)
     */
    public abstract ConstantAction getCreateViewConstantAction
    (
        String    schemaName,
        String            tableName,
        int                tableType,
        String            viewText,
        int                checkOption,
        ColumnInfo[]    columnInfo,
        ProviderInfo[]  providerInfo,
        UUID            compSchemaId);



    /**
     *    Make the ConstantAction for a Replicated DELETE statement.
     *
     *  @param conglomId            Conglomerate ID.
     *  @param tableType            type of this table
     *    @param heapSCOCI            StaticCompiledOpenConglomInfo for heap.
     *  @param irgs                    Index descriptors
     *  @param indexCIDS            Conglomerate IDs of indices
     *    @param indexSCOCIs    StaticCompiledOpenConglomInfos for indexes.
     *  @param emptyHeapRow            Template for heap row.
     *    @param deferred                True means deferred delete
     *  @param tableIsPublished        true if table is published
     *  @param tableID                table id
     *    @param lockMode                The lock mode to use
     *                                  (row or table, see TransactionController)
     *  @param keySignature         signature for the key(null for source)
     *  @param keyPositions         positions of primary key columns in base row
     *  @param keyConglomId          conglomerate id for the key
     *                                (-1 for the souce)
     *  @param schemaName            schemaName(null for source)
     *  @param tableName            tableName(null for source)
     *  @param resultDescription    A description of the columns in the row
     *            to be deleted.  Only set in replication or during cascade Delete.
     *    @param fkInfo                Array of structures containing foreign key
     *                                info, if any (may be null)
     *    @param triggerInfo            Array of structures containing trigger
     *                                info, if any (may be null)

     *  @param numColumns            Number of columns to read
     *  @param dependencyId            UUID for dependency system
     *  @param baseRowReadList      Map of columns read in.  1 based.
     *    @param baseRowReadMap        BaseRowReadMap[heapColId]->ReadRowColumnId.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
     *  @param singleRowSource        Whether or not source is a single row source
     *
     *  @exception StandardException        Thrown on failure
     */
    public abstract ConstantAction getDeleteConstantAction
    (
                                long                conglomId,
                                int                    tableType,
                                StaticCompiledOpenConglomInfo heapSCOCI,
                                int[] pkColumns,
                                IndexRowGenerator[]    irgs,
                                long[]                indexCIDS,
                                StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                ExecRow                emptyHeapRow,
                                boolean                deferred,
                                boolean                tableIsPublished,
                                UUID                tableID,
                                int                    lockMode,
                                Object                 deleteToken,
                                Object                 keySignature,
                                int[]                keyPositions,
                                long                keyConglomId,
                                String                schemaName,
                                String                tableName,
                                ResultDescription    resultDescription,
                                FKInfo[]            fkInfo,
                                TriggerInfo            triggerInfo,
                                FormatableBitSet                baseRowReadList,
                                int[]                baseRowReadMap,
                                int[]               streamStorableHeapColIds,
                                int                    numColumns,
                                UUID                dependencyId,
                                boolean                singleRowSource,
                                ConstantAction[]    dependentConstantActions
    )
            throws StandardException;


    /**
     *    Make ConstantAction to drop a constraint.
     *
     *  @param constraintName    Constraint name.
     *    @param constraintSchemaName        Constraint Schema Name
     *  @param tableName        Table name.
     *    @param tableId            UUID of table.
     *  @param tableSchemaName                the schema that table lives in.
     *  @param indexAction        IndexConstantAction for constraint (if necessary)
     *    @param behavior            The drop behavior (e.g. StatementType.RESTRICT)
     *  @param verifyType       Verify that the constraint is of this type.
     */
    public abstract ConstantAction    getDropConstraintConstantAction
    (
        String                    constraintName,
        String                    constraintSchemaName,
        String                    tableName,
        UUID                    tableId,
        String                    tableSchemaName,
        ConstantAction indexAction,
        int                        behavior,
        int                     verifyType
    );


    /**
     *    Make the ConstantAction for a DROP INDEX statement.
     *
     *
     *    @param    fullIndexName        Fully qualified index name
     *    @param    indexName            Index name.
     *    @param    tableName            The table name
     *    @param    schemaName                    Schema that index lives in.
     *  @param  tableId                UUID for table
     *  @param  tableConglomerateId    heap conglomerate ID for table
     *
     */
    public abstract    ConstantAction    getDropIndexConstantAction
    (
        String                fullIndexName,
        String                indexName,
        String                tableName,
        String                schemaName,
        UUID                tableId,
        long                tableConglomerateId
    );


    /**
     *    Make the ConstantAction for a DROP ALIAS statement.
     *
     *    @param    aliasName            Alias name.
     *    @param    aliasType            Alias type.
     */
    public abstract ConstantAction    getDropAliasConstantAction(SchemaDescriptor    sd, String aliasName, char aliasType);

    /**
     *    Make the ConstantAction for a DROP ROLE statement.
     *
     *    @param    roleName            role name to be dropped
     *
     */
    public abstract ConstantAction getDropRoleConstantAction(String roleName);

    /**
     *    Make the ConstantAction for a DROP SEQUENCE statement.
     *
     *  @param sd the schema the sequence object belongs to
     *    @param    seqName    name of sequence to be dropped
     *
     */
    public abstract ConstantAction getDropSequenceConstantAction(SchemaDescriptor sd, String seqName);


    /**
     *    Make the ConstantAction for a DROP SCHEMA statement.
     *
     *    @param    schemaName            Table name.
     */
    public abstract ConstantAction    getDropSchemaConstantAction(String    schemaName);


    /**
     *    Make the ConstantAction for a DROP TABLE statement.
     *
     *
     *    @param    fullTableName        Fully qualified table name
     *    @param    tableName            Table name.
     *    @param    sd                    Schema that table lives in.
     *  @param  conglomerateNumber    Conglomerate number for heap
     *  @param  tableId                UUID for table
     *  @param  behavior            drop behavior, CASCADE, RESTRICT or DEFAULT
     *
     */
    public abstract ConstantAction    getDropTableConstantAction
    (
        String                fullTableName,
        String                tableName,
        SchemaDescriptor    sd,
        long                conglomerateNumber,
        UUID                tableId,
        int                    behavior
    );

    /**
     *    Make the ConstantAction for a DROP TABLE statement.
     *
     *
     *    @param    fullTableName        Fully qualified table name
     *    @param    tableName            Table name.
     *    @param    sd                    Schema that table lives in.
     *  @param  conglomerateNumber    Conglomerate number for heap
     *  @param  tableId                UUID for table
     *  @param  behavior            drop behavior, CASCADE, RESTRICT or DEFAULT
     *
     */
    public abstract ConstantAction    getDropPinConstantAction
    (
            String                fullTableName,
            String                tableName,
            SchemaDescriptor    sd,
            long                conglomerateNumber,
            UUID                tableId,
            int                    behavior
    );


    /**
     *    Make the ConstantAction for a DROP VIEW statement.
     *
     *
     *    @param    fullTableName        Fully qualified table name
     *    @param    tableName            Table name.
     *    @param    sd                    Schema that view lives in.
     *
     */
    public abstract ConstantAction    getDropViewConstantAction
    (
        String                fullTableName,
        String                tableName,
        SchemaDescriptor    sd
    );

    /**
     *    Make the ConstantAction for a RENAME TABLE/COLUMN/INDEX statement.
     *
     *    @param    fullTableName Fully qualified table name
     *    @param    tableName   Table name.
     *    @param    oldObjectName   Old object name
     *    @param    newObjectName   New object name.
     *    @param    sd    Schema that table lives in.
     *    @param    tableId   UUID for table
     *  @param    usedAlterTable    True if used Alter Table command, false if used Rename
     *  @param    renamingWhat    Value indicates if Rename Column/Index.
     *
     */
    public abstract    ConstantAction    getRenameConstantAction
    (
        String                fullTableName,
        String                tableName,
        String                oldObjectName,
        String                newObjectName,
        SchemaDescriptor    sd,
        UUID                tableId,
        boolean                usedAlterTable,
        int                renamingWhat
    );

    /**
     *    Make the ConstantAction for a Replicated INSERT statement.
     *
     *  @param conglomId        Conglomerate ID.
     *  @param heapSCOCI        StaticCompiledOpenConglomInfo for target heap.
     *  @param irgs                Index descriptors
     *  @param indexCIDS        Conglomerate IDs of indices
     *    @param indexSCOCIs        StaticCompiledOpenConglomInfos for indexes.
     *  @param indexNames        Names of indices on this table for error
     *                            reporting.
     *    @param deferred            True means deferred insert
     *  @param tableIsPublished    true if table is published, false otherwise
     *  @param tableID            table id
     *  @param targetProperties    Properties on the target table
     *    @param fkInfo            Array of structures containing foreign key info,
     *                            if any (may be null)
     *    @param triggerInfo        Array of structures containing trigger info,
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
     *                            if any (may be null)
     *  @param indexedCols        boolean[] of which (0-based) columns are indexed.
     *  @param dependencyId        UUID for dependency system
     *    @param stageControl        Stage Control Tokens
     *    @param ddlList            List of DDL to log. This is for BULK INSERT into a published table at the Source.
     *  @param singleRowSource    Whether or not source is a single row source
     *  @param autoincRowLocation array of row locations into syscolumns for
                                  autoincrement columns
     *
     * @exception StandardException        Thrown on failure
     */
    public abstract    ConstantAction getInsertConstantAction(
                                TableDescriptor        tableDescriptor,
                                long                conglomId,
                                StaticCompiledOpenConglomInfo heapSCOCI,
                                int[] pkColumns,
                                IndexRowGenerator[]    irgs,
                                long[]                indexCIDS,
                                StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                String[]            indexNames,
                                boolean                deferred,
                                boolean                tableIsPublished,
                                UUID                tableID,
                                int                    lockMode,
                                Object                 insertToken,
                                Object                rowSignature,
                                Properties            targetProperties,
                                FKInfo[]            fkInfo,
                                TriggerInfo            triggerInfo,
                                int[]               streamStorableHeapColIds,
                                boolean[]            indexedCols,
                                UUID                dependencyId,
                                Object[]            stageControl,
                                Object[]            ddlList,
                                boolean                singleRowSource,
                                RowLocation[]        autoincRowLocation
                            )
            throws StandardException;

    /**
     *    Make the ConstantAction for an updatable VTI statement.
     *
     * @param deferred                    Deferred mode?
     */
    public abstract ConstantAction getUpdatableVTIConstantAction(int statementType, boolean deferred)
            throws StandardException;

    /**
     *    Make the ConstantAction for an updatable VTI statement.
     *
     * @param deferred                    Deferred mode?
     * @param changedColumnIds Array of ids of changed columns
     */
    public abstract ConstantAction getUpdatableVTIConstantAction( int statementType,
                                                         boolean deferred,
                                                         int[] changedColumnIds)
            throws StandardException;

    /**
     * Make the ConstantAction for a LOCK TABLE statement.
     *
     *  @param fullTableName        Full name of the table.
     *  @param conglomerateNumber    Conglomerate number for the heap
     *  @param exclusiveMode        Whether or not to get an exclusive lock.
     */
    public abstract ConstantAction    getLockTableConstantAction(
                    String fullTableName,
                    long conglomerateNumber, boolean exclusiveMode);


    /**
     * Make the ConstantAction for a SET SCHEMA statement.
     *
     *  @param schemaName    Name of schema.
     *  @param type            Literal, USER or ?
     */
    public abstract ConstantAction    getSetSchemaConstantAction(String schemaName, int type);

    /**
     * Make the ConstantAction for a SET TRANSACTION ISOLATION statement.
     *
     * @param isolationLevel    The new isolation level.
     */
    public abstract ConstantAction getSetTransactionIsolationConstantAction(int isolationLevel);

    /**
     * @param properties
     * @return
     */
    public abstract ConstantAction getSetSessionPropertyConstantAction(Properties properties);

    /**
     *    Make the ConstantAction for a Replicated DELETE statement.
     *
     *  @param conglomId            Conglomerate ID.
     *  @param tableType            type of this table
     *    @param heapSCOCI            StaticCompiledOpenConglomInfo for heap.
     *  @param irgs                    Index descriptors
     *  @param indexCIDS            Conglomerate IDs of indices
     *    @param indexSCOCIs    StaticCompiledOpenConglomInfos for indexes.
     *  @param emptyHeapRow            Template for heap row.
     *    @param deferred                True means deferred update
     *    @param targetUUID            UUID of target table
     *    @param lockMode                The lock mode to use
     *                                  (row or table, see TransactionController)
     *  @param tableIsPublished        true if table is published, false otherwise
     *    @param changedColumnIds        Array of ids of changes columns
     *  @param keyPositions         positions of primary key columns in base row
     *    @param fkInfo                Array of structures containing foreign key info,
     *                                if any (may be null)
     *    @param triggerInfo            Array of structures containing trigger info,
     *  @param baseRowReadList      Map of columns read in.  1 based.
     *  @param baseRowReadMap        map of columns to be selected from the base row
     *                                (partial row). 1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
     *  @param numColumns            The number of columns being read.
     *    @param positionedUpdate        is this a positioned update
     *  @param singleRowSource        Whether or not source is a single row source
     *
     *  @exception StandardException Thrown on failure
     */
    public abstract ConstantAction    getUpdateConstantAction(
                                long                conglomId,
                                int                    tableType,
                                StaticCompiledOpenConglomInfo heapSCOCI,
                                int[] pkColumns,
                                IndexRowGenerator[]    irgs,
                                long[]                indexCIDS,
                                StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                String[]            indexNames,
                                ExecRow                emptyHeapRow,
                                boolean                deferred,
                                UUID                targetUUID,
                                int                    lockMode,
                                boolean                tableIsPublished,
                                int[]                changedColumnIds,
                                int[]                keyPositions,
                                Object                 updateToken,
                                FKInfo[]            fkInfo,
                                TriggerInfo            triggerInfo,
                                FormatableBitSet                baseRowReadList,
                                int[]                baseRowReadMap,
                                int[]                streamStorableHeapColIds,
                                int                    numColumns,
                                boolean                positionedUpdate,
                                boolean                singleRowSource,
                                int[] storagePositionArray
                            )
            throws StandardException;

    /**
     *    Make the ConstantAction for a CREATE TRIGGER statement.
     *
     * @param triggerSchemaName        Name of the schema that trigger lives in.
     * @param triggerName    Name of trigger
     * @param eventMask        TriggerDescriptor.TRIGGER_EVENT_XXXX
     * @param isBefore        is this a before (as opposed to after) trigger
     * @param isRow            is this a row trigger or statement trigger
     * @param isEnabled        is this trigger enabled or disabled
     * @param triggerTable    the table upon which this trigger is defined
     * @param whenText        the text of the when clause (may be null)
     * @param actionTextList the text of the trigger actions (may be empty)
     * @param spsCompSchemaId    the compilation schema for the action and when
     *                            spses.   If null, will be set to the current default
     *                            schema
     * @param referencedCols    what columns does this trigger reference (may be null)
     * @param referencedColsInTriggerAction    what columns does the trigger
     *                        action reference through old/new transition variables
     *                        (may be null)
         * @param originalWhenText The original user text of the WHEN clause (may be null)
     * @param originalActionTextList The original user texts of the trigger actions
     * @param referencingOld whether or not OLD appears in REFERENCING clause
     * @param referencingNew whether or not NEW appears in REFERENCING clause
     * @param oldReferencingName old referencing table name, if any, that appears in REFERCING clause
     * @param newReferencingName new referencing table name, if any, that appears in REFERCING clause
     */
    public abstract ConstantAction getCreateTriggerConstantAction(
        String              triggerSchemaName,
        String              triggerName,
        TriggerEventDML     eventMask,
        boolean             isBefore,
        boolean             isRow,
        boolean             isEnabled,
        TableDescriptor     triggerTable,
        String              whenText,
        List<String>        actionTextList,
        UUID                spsCompSchemaId,
        int[]               referencedCols,
        int[]               referencedColsInTriggerAction,
        String              originalWhenText,
        List<String>        originalActionTextList,
        boolean             referencingOld,
        boolean             referencingNew,
        String              oldReferencingName,
        String              newReferencingName
    );

    /**
     * Make the ConstantAction for a DROP TRIGGER statement.
     *
     * @param    sd                    Schema that stored prepared statement lives in.
     * @param    triggerName            Name of the Trigger
     * @param    tableId                The table this trigger is defined upon
     */
    public abstract ConstantAction getDropTriggerConstantAction
    (
        SchemaDescriptor    sd,
        String                triggerName,
        UUID                tableId
    );

    /**
     * Make the constant action for Drop Statistics statement.
     *
     * @param sd            Schema Descriptor of the schema in which the object
     * resides.
     * @param fullTableName full name of the object for which statistics are
     * being dropped.
     * @param objectName     object name for which statistics are being dropped.
     * @param forTable          is it an index or table whose statistics aer being
     * consigned to the garbage heap?
     */
    public abstract ConstantAction getDropStatisticsConstantAction
        (SchemaDescriptor sd, String fullTableName, String objectName, boolean forTable);

    /**
     * Make the constant action for a Grant statement
     *
     * @param privileges The list of privileges to be granted
     * @param grantees The list of grantees
     */
    public abstract ConstantAction getGrantConstantAction( PrivilegeInfo privileges, List grantees);


    /**
     * Make the ConstantAction for a GRANT role statement.
     *
     * @param roleNames list of roles to be granted
     * @param grantees  list of authentication ids (user or roles) to
     *                  which roles(s) are to be granted
     */
    public abstract ConstantAction getGrantRoleConstantAction(List roleNames, List grantees, boolean isDefaultRole);


    /**
     * Make the constant action for a Revoke statement
     *
     * @param privileges The list of privileges to be revokeed
     * @param grantees The list of grantees
     */
    public abstract ConstantAction getRevokeConstantAction( PrivilegeInfo privileges, List grantees);

    /**
     * Make the ConstantAction for a REVOKE role statement.
     *
     * @param roleNames list of roles to be revoked
     * @param grantees  list of authentication ids (user or roles) for whom
     *                  roles are to be revoked
     */
    public abstract ConstantAction getRevokeRoleConstantAction(List roleNames, List grantees);

    public abstract ConstantAction[] createConstraintConstantActionArray(int size);
    
    public boolean primaryKeyConstantActionCheck(ConstantAction constantAction) {
        return (constantAction instanceof CreateConstraintConstantAction) && ((CreateConstraintConstantAction) constantAction).getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT;
    }
    
}
