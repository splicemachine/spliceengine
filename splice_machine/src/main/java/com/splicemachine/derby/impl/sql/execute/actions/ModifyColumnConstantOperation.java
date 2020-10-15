/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.PreparedStatement;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.RowUtil;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 9/3/14
 */
public class ModifyColumnConstantOperation extends AlterTableConstantOperation{
    private static final Logger LOG = Logger.getLogger(ModifyColumnConstantOperation.class);

    /**
     * Make the AlterAction for an ALTER TABLE statement.
     *
     * @param sd                     descriptor for the table's schema.
     * @param tableName              Name of table.
     * @param tableId                UUID of table
     * @param columnInfo             Information on all the columns in the table.
     * @param constraintActions      ConstraintConstantAction[] for constraints
     * @param lockGranularity        The lock granularity.
     * @param behavior               drop behavior for dropping column
     * @param indexNameForStatistics Will name the index whose statistics
     */
    public ModifyColumnConstantOperation(SchemaDescriptor sd, String tableName, UUID tableId,
                                         ColumnInfo[] columnInfo, ConstantAction[] constraintActions,
                                         char lockGranularity, int behavior,
                                         String indexNameForStatistics) {
        super(sd, tableName, tableId,
                columnInfo, constraintActions,
                behavior, indexNameForStatistics);
    }

    @Override protected boolean waitsForConcurrentTransactions() { return true; }

    @Override
    protected void executeConstantActionBody(Activation activation) throws StandardException {

        // Do all the DDL data dictionary prep work for ALTER_TABLE. This will suffice since we're doing in-place DDL changes.
        prepareDataDictionary(activation);

        executeColumnModificationAction(activation);
        // adjust dependencies on user defined types
        adjustUDTDependencies(activation, columnInfo, false );
        executeConstraintActions(activation);
    }

    private int executeColumnModificationAction(Activation activation) throws StandardException{
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        TransactionController tc = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        TableDescriptor td = activation.getDDLTableDescriptor();
        boolean tableNeedsScanning = false;
        boolean tableScanned = false;
        int numRows = -1;

        /* NOTE: We only allow a single column to be added within
         * each ALTER TABLE command at the language level.  However,
         * this may change some day, so we will try to plan for it.
         *
         * for each new column, see if the user is adding a non-nullable
         * column.  This is only allowed on an empty table.
         */
        for (ColumnInfo aColumnInfo : columnInfo) {
            /* Is this new column non-nullable?
             * If so, it can only be added to an
             * empty table if it does not have a default value.
             * We need to scan the table to find out how many rows
             * there are.
             */
            if (aColumnInfo.action == ColumnInfo.CREATE && !aColumnInfo.dataType.isNullable() &&
                    aColumnInfo.defaultInfo == null && aColumnInfo.defaultValue == null && aColumnInfo.autoincInc == 0) {
                tableNeedsScanning = true;
            }
        }

        // Scan the table if necessary
        if (tableNeedsScanning) {
            numRows = getSemiRowCount(tc,td);
            // Don't allow add of non-nullable column to non-empty table
            if (numRows > 0) {
                throw StandardException.newException(SQLState.LANG_ADDING_NON_NULL_COLUMN_TO_NON_EMPTY_TABLE,td.getQualifiedName());
            }
            tableScanned = true;
        }

        // for each related column, stuff system.column
        for (int ix = 0; ix < columnInfo.length; ix++) {
            /* If there is a default value, use it, otherwise use null */

            // Are we adding a new column or modifying a default?

            switch(columnInfo[ix].action){
                case ColumnInfo.CREATE:
                    addNewColumnToTable(activation, td, ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT:
                    modifyColumnDefault(lcc,dd,td,ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_TYPE:
                    modifyColumnType(dd,tc,td,ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_CONSTRAINT:
                    modifyColumnConstraint(activation,td,columnInfo[ix].name,true);
                    break;
                case ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL:
                    if(!tableScanned){
                        tableScanned=true;
                        numRows = getSemiRowCount(tc,td);
                    }
                    // check that the data in the column is not null
                    String colNames[]  = new String[1];
                    colNames[0]        = columnInfo[ix].name;
                    boolean nullCols[] = new boolean[1];

                    /* note validateNotNullConstraint returns true if the
                     * column is nullable
                     */
                    if (validateNotNullConstraint(colNames, nullCols, numRows, lcc, td,SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN)) {
                        /* nullable column - modify it to be not null
                         * This is O.K. at this point since we would have
                         * thrown an exception if any data was null
                         */
                        modifyColumnConstraint(activation,td,columnInfo[ix].name, false);
                    }
                    break;
                case ColumnInfo.DROP:
                    dropColumnFromTable(activation, td, columnInfo[ix].name);
                    break;
                default:
                    SanityManager.THROWASSERT("Unexpected action in AlterTableConstantAction");
            }
        }
        return numRows;
    }

    private void addNewColumnToTable(Activation activation, TableDescriptor tableDescriptor, int infoIndex) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        TransactionController tc = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        ColumnInfo colInfo = columnInfo[infoIndex];

        // Drop the table
        try {
        dd.dropTableDescriptor(tableDescriptor,sd,tc);
        } catch (StandardException e) {
            if (ErrorState.WRITE_WRITE_CONFLICT.getSqlState().equals(e.getSQLState())) {
                throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("AddColumn("+tableDescriptor.getQualifiedName()+"."+colInfo.name+")",
                                                                      e.getMessage());
            }
            throw e;
        }
        // Change the table name of the table descriptor
        tableDescriptor.setColumnSequence(tableDescriptor.getColumnSequence()+1);
        // add the table descriptor with new name
        dd.addDescriptor(tableDescriptor,sd,DataDictionary.SYSTABLES_CATALOG_NUM,false,tc,false);

        ColumnDescriptor columnDescriptor = tableDescriptor.getColumnDescriptor(colInfo.name);

         /* We need to verify that the table does not have an existing
         * column with the same name before we try to add the new
         * one as addColumnDescriptor() is a void method.
         */
        if (columnDescriptor != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,columnDescriptor.getDescriptorType(),
                    colInfo.name,tableDescriptor.getDescriptorType(),tableDescriptor.getQualifiedName());
        }
        DataValueDescriptor storableDV = colInfo.defaultValue != null?colInfo.defaultValue:colInfo.dataType.getNull();
        int storageNumber = tableDescriptor.getColumnSequence();
        int colNumber = tableDescriptor.getNumberOfColumns()+1;
        // Add the column to the conglomerate.(Column ids in store are 0-based)
        tc.addColumnToConglomerate(tableDescriptor.getHeapConglomerateId(), colNumber, storableDV, colInfo.dataType.getCollationType());




        // Generate a UUID for the default, if one exists and there is no default id yet.
        UUID defaultUUID = colInfo.newDefaultUUID;
        if (colInfo.defaultInfo != null && defaultUUID == null) {
            defaultUUID = dd.getUUIDFactory().createUUID();
        }

        // Add the column to syscolumns.
        // Column ids in system tables are 1-based
        columnDescriptor =  new ColumnDescriptor(colInfo.name,
                colNumber,
                storageNumber,
                colInfo.dataType,
                colInfo.defaultValue,
                colInfo.defaultInfo,
                tableDescriptor,
                defaultUUID,
                colInfo.autoincStart,
                colInfo.autoincInc,
                tableDescriptor.getColumnSequence());

        dd.addDescriptor(columnDescriptor, tableDescriptor, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);

        // now add the column to the tables column descriptor list.
        tableDescriptor.getColumnDescriptorList().add(columnDescriptor);

        if (columnDescriptor.isAutoincrement()) {
            updateNewAutoincrementColumn(lcc,tableDescriptor,colInfo);
        }

        // Update the new column to its default, if it has a non-null default
        boolean skipPhysicalUpdate = !columnDescriptor.hasNonNullDefault() ||
                columnDescriptor.getDefaultValue() != null &&   /* has default value */
                !columnDescriptor.isAutoincrement() && /* not an auto-increment column */
                !columnDescriptor.hasGenerationClause() && /* not a generated column */
                !columnDescriptor.getType().isNullable(); /* is not nullable */

        if (!skipPhysicalUpdate) {
            updateNewColumnToDefault(columnDescriptor, tableDescriptor, lcc);
        }
        //
        // Add dependencies. These can arise if a generated column depends
        // on a user created function.
        //
        addColumnDependencies( lcc, dd, tableDescriptor, colInfo);

        // Update SYSCOLPERMS table which tracks the permissions granted
        // at columns level. The system table has a bit map of all the columns
        // in the user table to help determine which columns have the
        // permission granted on them. Since we are adding a new column,
        // that bit map needs to be expanded and initialize the bit for it
        // to 0 since at the time of ADD COLUMN, no permissions have been
        // granted on that new column.
        //
        dd.updateSYSCOLPERMSforAddColumnToUserTable(tableDescriptor.getUUID(), tc);

        // refresh the activation's TableDescriptor now that we've modified it
        activation.setDDLTableDescriptor(tableDescriptor);
    }

    private void updateNewAutoincrementColumn(LanguageConnectionContext lcc, TableDescriptor td,ColumnInfo colInfo) throws StandardException {
        String columnName = colInfo.name;
        long initial = colInfo.autoincStart;
        long increment = colInfo.autoincInc;
        /*
         * Update values in a new autoincrement column being added to a table.
         * This is similar to updateNewColumnToDefault whereby we issue an
         * update statement using a nested connection. The UPDATE statement
         * uses a static method in ConnectionInfo (which is not documented)
         * which returns the next value to be inserted into the autoincrement
         * column.
         *
         * @param columnName autoincrement column name that is being added.
         * @param initial    initial value of the autoincrement column.
         * @param increment  increment value of the autoincrement column.
         *
         * @see #updateNewColumnToDefault
         */
        // Don't throw an error in bind when we try to update the
        // autoincrement column.
        lcc.setAutoincrementUpdate(true);

        lcc.autoincrementCreateCounter(td.getSchemaName(),
                                       td.getName(),
                                       columnName, initial,
                                       increment, 0);
        // the sql query is.
        // UPDATE table
        //  set ai_column = ConnectionInfo.nextAutoincrementValue(
        //                            schemaName, tableName,
        //                            columnName)
        String updateStmt = "UPDATE " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
                " SET " + IdUtil.normalToDelimited(columnName) + "=" +
                "com.splicemachine.db.iapi.db.ConnectionInfo::" +
                "nextAutoincrementValue(" +
                StringUtil.quoteStringLiteral(td.getSchemaName()) + "," +
                StringUtil.quoteStringLiteral(td.getName()) + "," +
                StringUtil.quoteStringLiteral(columnName) + ")";



        try {
            AlterTableConstantOperation.executeUpdate(lcc, updateStmt);
        } catch (StandardException se)
        {
            if (se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE)) {
                // If overflow, override with more meaningful message.
                throw StandardException.newException(SQLState.LANG_AI_OVERFLOW,
                        se,
                        td.getName(),
                        columnName);
            }
            throw se;
        } finally {
            // and now update the autoincrement value.
            lcc.autoincrementFlushCache(td.getUUID());
            lcc.setAutoincrementUpdate(false);
        }

    }

    /**
     * Update a new column with its default.
     * We could do the scan ourself here, but
     * instead we get a nested connection and
     * issue the appropriate update statement.
     *
     * @param columnDescriptor  catalog descriptor for the column
     *
     * @exception StandardException if update to default fails
     */
    private void updateNewColumnToDefault(ColumnDescriptor columnDescriptor, TableDescriptor td, LanguageConnectionContext lcc)
        throws StandardException {
        DefaultInfo defaultInfo = columnDescriptor.getDefaultInfo();
        String columnName = columnDescriptor.getColumnName();
        String defaultText;

        if ( defaultInfo == null || defaultInfo.isGeneratedColumn() ) {
            defaultText = "default";
        } else {
            defaultText = defaultInfo.getDefaultText();
        }

        /* Need to use delimited identifiers for all object names
         * to ensure correctness.
         */
        String updateStmt = "UPDATE " +
            IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
            " SET " + IdUtil.normalToDelimited(columnName) + "=" +
            defaultText;

        executeUpdate(lcc, updateStmt);
    }

    private void updateNonPhysicallyPopulatedColumnToDefault(ColumnDescriptor columnDescriptor, TableDescriptor td, LanguageConnectionContext lcc)
            throws StandardException {
        String  columnName = columnDescriptor.getColumnName();
        String  defaultText;

        defaultText = columnDescriptor.getDefaultInfo().getDefaultText();

        /* Need to use delimited identifiers for all object names
         * to ensure correctness.
         */
        String updateStmt = "UPDATE " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
                " SET " + IdUtil.normalToDelimited(columnName) + "=" +
                defaultText + " where " + IdUtil.normalToDelimited(columnName) + " = " + defaultText;


        executeUpdate(lcc, updateStmt);
    }
    private void modifyColumnDefault(LanguageConnectionContext lcc, DataDictionary dd,TableDescriptor td,int ix) throws StandardException {
        ColumnInfo colInfo = columnInfo[ix];
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(colInfo.name);

        boolean needUpdate = columnDescriptor.hasNonNullDefault() &&
                        columnDescriptor.getDefaultValue() != null &&   /* has default value */
                        !columnDescriptor.isAutoincrement() && /* not an auto-increment column */
                        !columnDescriptor.hasGenerationClause() && /* not a generated column */
                        !columnDescriptor.getType().isNullable(); /* is not nullable */
        if (needUpdate)
            updateNonPhysicallyPopulatedColumnToDefault(columnDescriptor, td, lcc);

        int columnPosition = columnDescriptor.getPosition();
        int storagePosition = columnDescriptor.getStoragePosition();

        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();
        // Clean up after the old default, if non-null
        if (columnDescriptor.hasNonNullDefault()) {
            // Invalidate off of the old default
            DefaultDescriptor defaultDescriptor = new DefaultDescriptor(dd, colInfo.oldDefaultUUID,
                    td.getUUID(), columnPosition);


            dm.invalidateFor(defaultDescriptor, DependencyManager.MODIFY_COLUMN_DEFAULT, lcc);

            // Drop any dependencies
            dm.clearDependencies(lcc, defaultDescriptor);
        }

        UUID defaultUUID = colInfo.newDefaultUUID;

        /* Generate a UUID for the default, if one exists
         * and there is no default id yet.
         */
        if (colInfo.defaultInfo != null && defaultUUID == null) {
            defaultUUID = dd.getUUIDFactory().createUUID();
        }

        /* Get a ColumnDescriptor reflecting the new default */
        columnDescriptor = new ColumnDescriptor(
                colInfo.name,
                columnPosition,
                storagePosition,
                colInfo.dataType,
                colInfo.defaultValue,
                colInfo.defaultInfo,
                td,
                defaultUUID,
                colInfo.autoincStart,
                colInfo.autoincInc,
                colInfo.autoinc_create_or_modify_Start_Increment,
                colInfo.partitionPosition,
                (byte)0
        );

        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), colInfo.name, tc);
        dd.addDescriptor(columnDescriptor, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);

    }

    private void modifyColumnType(DataDictionary dd, TransactionController tc,TableDescriptor td,int ix) throws StandardException {
        ColumnDescriptor columnDescriptor =
                td.getColumnDescriptor(columnInfo[ix].name),
                newColumnDescriptor;

        LOG.warn("Need to update TD!");
        newColumnDescriptor =
                new ColumnDescriptor(columnInfo[ix].name,
                        columnDescriptor.getPosition(),
                        columnDescriptor.getStoragePosition(),
                        columnInfo[ix].dataType,
                        columnDescriptor.getDefaultValue(),
                        columnDescriptor.getDefaultInfo(),
                        td,
                        columnDescriptor.getDefaultUUID(),
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc,
                        td.getColumnSequence()+1
                );



        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
        dd.addDescriptor(newColumnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
    }

    private boolean validateNotNullConstraint (
            String                            columnNames[],
            boolean                            nullCols[],
            int                                numRows,
            LanguageConnectionContext        lcc,
            TableDescriptor td,
            String                            errorMsg ) throws StandardException {
        boolean foundNullable = false;
        StringBuilder constraintText = new StringBuilder();

            /*
             * Check for nullable columns and create a constraint string which can
             * be used in validateConstraint to check whether any of the
             * data is null.
             */
        for (int colCtr = 0; colCtr < columnNames.length; colCtr++) {
            ColumnDescriptor cd = td.getColumnDescriptor(columnNames[colCtr]);

            if (cd == null) {
                throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnNames[colCtr],td.getName());
            }

            if (cd.getType().isNullable()) {
                if (numRows > 0) {
                    // already found a nullable column so add "AND"
                    if (foundNullable)
                        constraintText.append(" AND ");
                    // Delimiting the column name is important in case the
                    // column name uses lower case characters, spaces, or
                    // other unusual characters.
                    constraintText.append(IdUtil.normalToDelimited(columnNames[colCtr])).append(" IS NOT NULL ");
                }
                foundNullable = true;
                nullCols[colCtr] = true;
            }
        }

            /* if the table has nullable columns and isn't empty
             * we need to validate the data
             */
        if (foundNullable && numRows > 0) {
            if (!ConstraintConstantOperation.validateConstraint(null,constraintText.toString(),td,lcc,false)) {
                if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT)) {    //alter table add primary key
                    //soft upgrade mode
                    throw ErrorState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT.newException(td.getQualifiedName());
                } else if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY)) {    //alter table add primary key
                    throw ErrorState.LANG_NULL_DATA_IN_PRIMARY_KEY.newException(td.getQualifiedName());
                } else {    //alter table modify column not null
                    throw ErrorState.LANG_NULL_DATA_IN_NON_NULL_COLUMN.newException(td.getQualifiedName(),columnNames[0]);
                }
            }
        }
        return foundNullable;
    }

    private void modifyColumnConstraint(Activation activation,TableDescriptor td, String colName, boolean nullability) throws StandardException {
        /*
         * Workhorse for modifying column level constraints.
         * Right now it is restricted to modifying a null constraint to a not null
         * constraint.
         */
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = activation.getTransactionController();
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(colName), newColumnDescriptor;

        boolean needUpdate = nullability &&
                columnDescriptor.hasNonNullDefault() &&
                columnDescriptor.getDefaultValue() != null &&   /* has default value */
                !columnDescriptor.isAutoincrement() && /* not an auto-increment column */
                !columnDescriptor.hasGenerationClause() && /* not a generated column */
                !columnDescriptor.getType().isNullable(); /* is not nullable */
        if (needUpdate)
            updateNonPhysicallyPopulatedColumnToDefault(columnDescriptor, td, lcc);

        // Get the type and change the nullability
        DataTypeDescriptor dataType = columnDescriptor.getType().getNullabilityType(nullability);

        //check if there are any unique constraints to update
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
        int columnPostion = columnDescriptor.getPosition();
        for (int i = 0; i < cdl.size(); i++) {
            ConstraintDescriptor cd = cdl.elementAt(i);
            if (cd.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
                ColumnDescriptorList columns = cd.getColumnDescriptors();
                for (int count = 0; count < columns.size(); count++) {
                    if (columns.elementAt(count).getPosition() != columnPostion)
                        break;

                    //get backing index
                    ConglomerateDescriptor desc = td.getConglomerateDescriptor(cd.getConglomerateId());

                    //check if the backing index was created when the column
                    //not null ie is backed by unique index
                    if (!desc.getIndexDescriptor().isUnique())
                        break;

                    // replace backing index with a unique when not null index.
                    recreateUniqueConstraintBackingIndexAsUniqueWhenNotNull( desc, td, activation, lcc);
                }
            }
        }
        LOG.warn("Something not right here");
        newColumnDescriptor = new ColumnDescriptor(colName,
                        columnDescriptor.getPosition(),
                columnDescriptor.getStoragePosition(),
                        dataType,
                        columnDescriptor.getDefaultValue(),
                        columnDescriptor.getDefaultInfo(),
                        td,
                        columnDescriptor.getDefaultUUID(),
                        columnDescriptor.getAutoincStart(),
                        columnDescriptor.getAutoincInc(),
                    columnDescriptor.getPosition());

        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), colName, tc);
        dd.addDescriptor(newColumnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc, false);
    }

    private long getColumnMax(LanguageConnectionContext lcc,TableDescriptor td, String columnName, long increment) throws StandardException {
        /*
         * computes the minimum/maximum value in a column of a table.
         */
        String maxStr = (increment > 0) ? "MAX" : "MIN";
        String maxStmt = "SELECT  " + maxStr + "(" +
                IdUtil.normalToDelimited(columnName) + ") FROM " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName());

        PreparedStatement ps = lcc.prepareInternalStatement(maxStmt);

        // This is a substatement, for now we do not set any timeout for it
        // We might change this later by linking timeout to parent statement
        ResultSet rs = ps.executeSubStatement(lcc, false, 0L);
        DataValueDescriptor[] rowArray = rs.getNextRow().getRowArray();
        rs.close();
        rs.finish();
        return rowArray[0].getLong();
    }

    /**
     * Workhorse for dropping a column from a table.
     *
     * This routine drops a column from a table, taking care
     * to properly handle the various related schema objects.
     *
     * The syntax which gets you here is:
     *
     *   ALTER TABLE tbl DROP [COLUMN] col [CASCADE|RESTRICT]
     *
     * The keyword COLUMN is optional, and if you don't
     * specify CASCADE or RESTRICT, the default is CASCADE
     * (the default is chosen in the parser, not here).
     *
     * If you specify RESTRICT, then the column drop should be
     * rejected if it would cause a dependent schema object
     * to become invalid.
     *
     * If you specify CASCADE, then the column drop should
     * additionally drop other schema objects which have
     * become invalid.
     *
     * You may not drop the last (only) column in a table.
     *
     * Schema objects of interest include:
     *  - views
     *  - triggers
     *  - constraints
     *    - check constraints
     *    - primary key constraints
     *    - foreign key constraints
     *    - unique key constraints
     *    - not null constraints
     *  - privileges
     *  - indexes
     *  - default values
     *
     * Dropping a column may also change the column position
     * numbers of other columns in the table, which may require
     * fixup of schema objects (such as triggers and column
     * privileges) which refer to columns by column position number.
     *
     * Indexes are a bit interesting. The official SQL spec
     * doesn't talk about indexes; they are considered to be
     * an imlementation-specific performance optimization.
     * The current Derby behavior is that:
     *  - CASCADE/RESTRICT doesn't matter for indexes
     *  - when a column is dropped, it is removed from any indexes
     *    which contain it.
     *  - if that column was the only column in the index, the
     *    entire index is dropped.
     *
     * @param   columnName the name of the column specfication in the ALTER
     *                        statement-- currently we allow only one.
     * @exception StandardException     thrown on failure.
     */
    @SuppressWarnings("unchecked")
    private void dropColumnFromTable(Activation activation,
                                     TableDescriptor tableDescriptor,
                                     String columnName) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        boolean cascade = (behavior == StatementType.DROP_CASCADE);
        // drop any generated columns which reference this column
        ColumnDescriptorList generatedColumnList = tableDescriptor.getGeneratedColumns();
        int generatedColumnCount = generatedColumnList.size();
        List<String> cascadedDroppedColumns = new ArrayList<>(generatedColumnCount);
        for ( int i = 0; i < generatedColumnCount; i++ ) {
            ColumnDescriptor generatedColumn = generatedColumnList.elementAt( i );
            String[] referencedColumnNames = generatedColumn.getDefaultInfo().getReferencedColumnNames();
            for (String referencedColumnName : referencedColumnNames) {
                if (columnName.equals(referencedColumnName)) {
                    String generatedColumnName = generatedColumn.getColumnName();

                    // ok, the current generated column references the column
                    // we're trying to drop
                    if (!cascade) {
                        // Reject the DROP COLUMN, because there exists a
                        // generated column which references this column.
                        //
                        throw ErrorState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.newException(
                                dm.getActionString(DependencyManager.DROP_COLUMN),
                                columnName,"GENERATED COLUMN", generatedColumnName);
                    } else {
                        cascadedDroppedColumns.add(generatedColumnName);
                    }
                }
            }
        }

        // can NOT drop a column if it is the only one in the table
        if ((tableDescriptor.getColumnDescriptorList().size() - cascadedDroppedColumns.size()) == 1) {
            throw ErrorState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.newException(
                    dm.getActionString(DependencyManager.DROP_COLUMN),
                    "THE *LAST* COLUMN " + columnName,"TABLE",tableDescriptor.getQualifiedName());
        }

        // now drop dependent generated columns
        for (String generatedColumnName : cascadedDroppedColumns) {
            activation.addWarning(StandardException.newWarning(SQLState.LANG_GEN_COL_DROPPED, generatedColumnName, tableDescriptor.getName()));

            //
            // We can only recurse 2 levels since a generation clause cannot
            // refer to other generated columns.
            //
            dropColumnFromTable(activation,tableDescriptor,generatedColumnName);
        }

        /*
         * Cascaded drops of dependent generated columns may require us to
         * rebuild the table descriptor.
         */
        tableDescriptor = dd.getTableDescriptor(tableId);
        TransactionController tc = lcc.getTransactionExecute();

        ColumnDescriptor columnDescriptor = tableDescriptor.getColumnDescriptor( columnName );

        // We already verified this in bind, but do it again
        if (columnDescriptor == null) {
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName,tableDescriptor.getQualifiedName());
        }

//        try {
            tc.dropColumnFromConglomerate(tableDescriptor.getHeapConglomerateId(), columnDescriptor.getPosition());
//        } catch (StandardException e) {
//            if (ErrorState.WRITE_WRITE_CONFLICT.getSqlState().equals(e.getSQLState())) {
//                throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("DropColumn("+tableDescriptor.getQualifiedName()+"."+columnName+")",
//                                                                      e.getMessage());
//            }
//            throw e;
//        }

        int size = tableDescriptor.getColumnDescriptorList().size();
        int droppedColumnPosition = columnDescriptor.getPosition();

        FormatableBitSet toDrop = new FormatableBitSet(size + 1);
        toDrop.set(droppedColumnPosition);
        tableDescriptor.setReferencedColumnMap(toDrop);

        // If column has a default we drop the default and any dependencies
        if (columnDescriptor.getDefaultInfo() != null) {
            dm.clearDependencies(lcc, columnDescriptor.getDefaultDescriptor(dd));
        }

        //Now go through each trigger on this table and see if the column
        //being dropped is part of it's trigger columns or trigger action
        //columns which are used through REFERENCING clause
        handleTriggers(activation, tableDescriptor, columnName, droppedColumnPosition, lcc, dd,
                       dm.getActionString(DependencyManager.DROP_COLUMN), cascade, tc);

        // Now handle constraints
        List<ConstantAction> newCongloms = handleConstraints(activation, tableDescriptor, columnName, lcc, dd, dm, cascade, tc,
                                                             droppedColumnPosition);

        /* If there are new backing conglomerates which must be
         * created to replace a dropped shared conglomerate
         * (where the shared conglomerate was dropped as part
         * of a "drop constraint" call above), then create them
         * now.  We do this *after* dropping all dependent
         * constraints because we don't want to waste time
         * creating a new conglomerate if it's just going to be
         * dropped again as part of another "drop constraint".
         */
        createNewBackingCongloms(activation,tableDescriptor,newCongloms, null);

        /*
         * The work we've done above, specifically the possible
         * dropping of primary key, foreign key, and unique constraints
         * and their underlying indexes, may have affected the table
         * descriptor. By re-reading the table descriptor here, we
         * ensure that the compressTable code is working with an
         * accurate table descriptor. Without this line, we may get
         * conglomerate-not-found errors and the like due to our
         * stale table descriptor.
         */
        tableDescriptor = dd.getTableDescriptor(tableId);

        ColumnDescriptorList tab_cdl = tableDescriptor.getColumnDescriptorList();

        // drop the column from syscolumns
        dd.dropColumnDescriptor(tableDescriptor.getUUID(), columnName, tc);
        ColumnDescriptor[] cdlArray =
                new ColumnDescriptor[size - columnDescriptor.getPosition()];


        // For each column in this table with a higher column position,
        // drop the entry from SYSCOLUMNS, but hold on to the column
        // descriptor and reset its position to adjust for the dropped
        // column. Then, re-add all those adjusted column descriptors
        // back to SYSCOLUMNS
        //
        for(int i=columnDescriptor.getPosition(), j=0;i<size;i++,j++){
            ColumnDescriptor cd=tab_cdl.elementAt(i);
            dd.dropColumnDescriptor(tableDescriptor.getUUID(),cd.getColumnName(),tc);
            cd.setPosition(i);
            if(cd.isAutoincrement()){
                cd.setAutoinc_create_or_modify_Start_Increment(ColumnDefinitionNode.CREATE_AUTOINCREMENT);
            }
            cdlArray[j]=cd;
        }
        dd.addDescriptorArray(cdlArray,tableDescriptor,DataDictionary.SYSCOLUMNS_CATALOG_NUM,false,tc);

        List depsOnAlterTableList = dd.getProvidersDescriptorList(tableDescriptor.getObjectID().toString());
        for (Object aDepsOnAlterTableList : depsOnAlterTableList) {
            //Go through all the dependent objects on the table being altered
            DependencyDescriptor depOnAlterTableDesc =
                    (DependencyDescriptor) aDepsOnAlterTableList;
            DependableFinder dependent = depOnAlterTableDesc.getDependentFinder();
            //For the given dependent, we are only interested in it if it is a
            // stored prepared statement.
            if (dependent.getSQLObjectType().equals(Dependable.STORED_PREPARED_STATEMENT)) {
                //Look for all the dependent objects that are using this
                // stored prepared statement as provider. We are only
                // interested in dependents that are triggers.
                List depsTrigger = dd.getProvidersDescriptorList(depOnAlterTableDesc.getUUID().toString());
                for (Object aDepsTrigger : depsTrigger) {
                    DependencyDescriptor depsTriggerDesc =
                            (DependencyDescriptor) aDepsTrigger;
                    DependableFinder providerIsTrigger = depsTriggerDesc.getDependentFinder();
                    //For the given dependent, we are only interested in it if
                    // it is a trigger
                    if (providerIsTrigger.getSQLObjectType().equals(Dependable.TRIGGER)) {
                        //Drop and recreate the trigger after regenerating
                        // it's trigger action plan. If the trigger action
                        // depends on the column being dropped, it will be
                        // caught here.
                        TriggerDescriptor trdToBeDropped = dd.getTriggerDescriptor(depsTriggerDesc.getUUID());
                        UUID whenClauseId = trdToBeDropped.getWhenClauseId();
                        boolean gotDropped = false;
                        if (whenClauseId != null) {
                            gotDropped = columnDroppedAndTriggerDependencies(
                                    trdToBeDropped, tableDescriptor, whenClauseId,
                                    -1, cascade, columnName, activation);
                        }
                        // If no dependencies were found in the WHEN clause,
                        // we have to check if the triggered SQL statement
                        // depends on the column being dropped. But if there
                        // were dependencies and the trigger has already been
                        // dropped, there is no point in looking for more
                        // dependencies.
                        for (int i = 0; i < trdToBeDropped.getTriggerDefinitionSize() && !gotDropped; ++i) {
                            gotDropped = columnDroppedAndTriggerDependencies(trdToBeDropped, tableDescriptor,
                                    trdToBeDropped.getActionId(i),
                                    i, cascade, columnName, activation);
                        }

                    }
                }
            }
        }
        // Adjust the column permissions rows in SYSCOLPERMS to reflect the
        // changed column positions due to the dropped column:
        dd.updateSYSCOLPERMSforDropColumn(tableDescriptor.getUUID(), tc, columnDescriptor);

        // remove column descriptor from table descriptor. this fixes up the
        // list in case we were called recursively in order to cascade-drop a
        // dependent generated column.
        tab_cdl.remove( tableDescriptor.getColumnDescriptor( columnName ) );
    }

    private List<ConstantAction> handleConstraints(Activation activation,
                                                   TableDescriptor td,
                                                   String columnName,
                                                   LanguageConnectionContext lcc,
                                                   DataDictionary dd,
                                                   DependencyManager dm,
                                                   boolean cascade,
                                                   TransactionController tc,
                                                   int droppedColumnPosition) throws StandardException {
        ConstraintDescriptorList csdl = dd.getConstraintDescriptors(td);
        int csdl_size = csdl.size();

        List<ConstantAction> newCongloms = new ArrayList<>();

        // we want to remove referenced primary/unique keys in the second
        // round.  This will ensure that self-referential constraints will
        // work OK.
        int tbr_size = 0;
        ConstraintDescriptor[] toBeRemoved = new ConstraintDescriptor[csdl_size];

        // let's go downwards, don't want to get messed up while removing
        for (int i = csdl_size - 1; i >= 0; i--) {
            ConstraintDescriptor cd = csdl.elementAt(i);
            int[] referencedColumns = cd.getReferencedColumns();
            int numRefCols = referencedColumns.length, j;
            boolean changed = false;
            for (j = 0; j < numRefCols; j++) {
                if (referencedColumns[j] > droppedColumnPosition)
                    changed = true;
                if (referencedColumns[j] == droppedColumnPosition)
                    break;
            }
            if (j == numRefCols) {// column not referenced
                if ((cd instanceof CheckConstraintDescriptor) && changed) {
                    dd.dropConstraintDescriptor(cd, tc);
                    for (j = 0; j < numRefCols; j++) {
                        if (referencedColumns[j] > droppedColumnPosition)
                            referencedColumns[j]--;
                    }
                    ((CheckConstraintDescriptor) cd).setReferencedColumnsDescriptor(new ReferencedColumnsDescriptorImpl(referencedColumns));
                    dd.addConstraintDescriptor(cd, tc);
                }
                continue;
            }

            if (! cascade) {
                // Reject the DROP COLUMN, because there exists a constraint
                // which references this column.
                //
                throw ErrorState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.newException(
                        SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                        dm.getActionString(DependencyManager.DROP_COLUMN),
                        columnName, "CONSTRAINT",
                        cd.getConstraintName() );
            }

            if (cd instanceof ReferencedKeyConstraintDescriptor) {
                // restrict will raise an error in invalidate if referenced
                toBeRemoved[tbr_size++] = cd;
                continue;
            }

            // drop now in all other cases
            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);

            dropConstraint(cd, td, newCongloms, activation, lcc, true);
            activation.addWarning( StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                            cd.getConstraintName(), td.getName()));
        }

        for (int i = tbr_size - 1; i >= 0; i--) {
            ConstraintDescriptor cd = toBeRemoved[i];
            dropConstraint(cd, td, newCongloms, activation, lcc, false);

            activation.addWarning( StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                            cd.getConstraintName(), td.getName()));

            if (cascade) {
                ConstraintDescriptorList fkcdl = dd.getForeignKeys(cd.getUUID());
                for (int j = 0; j < fkcdl.size(); j++) {
                    ConstraintDescriptor fkcd = fkcdl.elementAt(j);
                    dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);

                    dropConstraint(fkcd, td, newCongloms, activation, lcc, true);

                    activation.addWarning( StandardException.newWarning(
                                    SQLState.LANG_CONSTRAINT_DROPPED,
                                    fkcd.getConstraintName(),
                                    fkcd.getTableDescriptor().getName()));
                }
            }

            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
            dm.clearDependencies(lcc, cd);
        }
        return newCongloms;
    }

    /**
     * Handle triggers on the table.  It may be that a trigger is on the column being dropped, in
     * which case we can drop the trigger too, or it the trigger could be on an adjacent column,
     * in which case we have to move the adjacent column and its trigger too.
     * @param activation the activation in case we need to set some warnings.
     * @param td descriptor of the table we're altering.
     * @param columnName the name of the column we're working on
     * @param droppedColumnPosition the zero-based position of the column in the table
     * @param lcc required for trigger drop
     * @param dd required for trigger drop
     * @param dropColumnActionString string used in exception msgs
     * @param cascade drop dependencies too?
     * @param tc used to assure triggers are dropped transitionally
     * @throws StandardException
     */
    private void handleTriggers(Activation activation,
                                TableDescriptor td,
                                String columnName,
                                int droppedColumnPosition,
                                LanguageConnectionContext lcc,
                                DataDictionary dd,
                                String dropColumnActionString,
                                boolean cascade,
                                TransactionController tc) throws StandardException {
        GenericDescriptorList tdl = dd.getTriggerDescriptors(td);
        for (Object aTdl : tdl) {
            TriggerDescriptor trd = (TriggerDescriptor) aTdl;
            //If we find that the trigger is dependent on the column being
            //dropped because column is part of trigger columns list, then
            //we will give a warning or drop the trigger based on whether
            //ALTER TABLE DROP COLUMN is RESTRICT or CASCADE. In such a
            //case, no need to check if the trigger action columns referenced
            //through REFERENCING clause also used the column being dropped.
            boolean triggerDroppedAlready = false;

            int[] referencedCols = trd.getReferencedCols();
            if (referencedCols != null) {
                int refColLen = referencedCols.length, j;
                boolean changed = false;
                for (j = 0; j < refColLen; j++) {
                    if (referencedCols[j] > droppedColumnPosition) {
                        //Trigger is not defined on the column being dropped
                        //but the column position of trigger column is changing
                        //because the position of the column being dropped is
                        //before the the trigger column
                        changed = true;
                    } else if (referencedCols[j] == droppedColumnPosition) {
                        //the trigger is defined on the column being dropped
                        if (cascade) {
                            trd.drop(lcc);
                            triggerDroppedAlready = true;
                            activation.addWarning(
                                    StandardException.newWarning(
                                            SQLState.LANG_TRIGGER_DROPPED,
                                            trd.getName(), td.getName()));
                        } else {  // we'd better give an error if don't drop it,
                            // otherwsie there would be unexpected behaviors
                            throw ErrorState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.newException(
                                    SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                                    dropColumnActionString,
                                    columnName, "TRIGGER",
                                    trd.getName());
                        }
                        break;
                    }
                }

                // The following if condition will be true if the column
                // getting dropped is not a trigger column, but one or more
                // of the trigger column's position has changed because of
                // drop column.
                if (j == refColLen && changed) {
                    dd.dropTriggerDescriptor(trd, tc);
                    for (j = 0; j < refColLen; j++) {
                        if (referencedCols[j] > droppedColumnPosition)
                            referencedCols[j]--;
                    }
                    dd.addDescriptor(trd, sd,
                            DataDictionary.SYSTRIGGERS_CATALOG_NUM,
                            false, tc, false);
                }
            }

            // If the trigger under consideration got dropped through the
            // loop above, then move to next trigger
            if (triggerDroppedAlready) continue;

            // Column being dropped is not one of trigger columns. Check if
            // that column is getting used inside the trigger action through
            // REFERENCING clause. This can be tracked only for triggers
            // created in 10.7 and higher releases. Derby releases prior to
            // that did not keep track of trigger action columns used
            // through the REFERENCING clause.
            int[] referencedColsInTriggerAction = trd.getReferencedColsInTriggerAction();
            if (referencedColsInTriggerAction != null) {
                int refColInTriggerActionLen = referencedColsInTriggerAction.length, j;
                boolean changedColPositionInTriggerAction = false;
                for (j = 0; j < refColInTriggerActionLen; j++) {
                    if (referencedColsInTriggerAction[j] > droppedColumnPosition) {
                        changedColPositionInTriggerAction = true;
                    } else if (referencedColsInTriggerAction[j] == droppedColumnPosition) {
                        if (cascade) {
                            trd.drop(lcc);
                            activation.addWarning(
                                    StandardException.newWarning(
                                            SQLState.LANG_TRIGGER_DROPPED,
                                            trd.getName(), td.getName()));
                        } else {  // we'd better give an error if don't drop it,
                            throw StandardException.newException(
                                    SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                                    dropColumnActionString,
                                    columnName, "TRIGGER",
                                    trd.getName());
                        }
                        break;
                    }
                }

                // change trigger to refer to columns in new positions
                // The following if condition will be true if the column
                // getting dropped is not getting used in the trigger action
                // sql through the REFERENCING clause but one or more of those
                // column's position has changed because of drop column.
                // This applies only to triggers created with 10.7 and higher.
                // Prior to that, Derby did not keep track of the trigger
                // action column used through the REFERENCING clause. Such
                // triggers will be caught later on in this method after the
                // column has been actually dropped from the table descriptor.
                if (j == refColInTriggerActionLen && changedColPositionInTriggerAction) {
                    dd.dropTriggerDescriptor(trd, tc);
                    for (j = 0; j < refColInTriggerActionLen; j++) {
                        if (referencedColsInTriggerAction[j] > droppedColumnPosition)
                            referencedColsInTriggerAction[j]--;
                    }
                    dd.addDescriptor(trd, sd,
                            DataDictionary.SYSTRIGGERS_CATALOG_NUM,
                            false, tc, false);
                }
            }
        }
    }


    private boolean
    columnDroppedAndTriggerDependencies(TriggerDescriptor trd,
                                        TableDescriptor td,
                                        UUID spsUUID, int index,
                            boolean cascade, String columnName,
                                        Activation activation)
            throws StandardException {
        boolean isWhenClause = index == -1;
        /*
         * For the trigger, get the trigger action sql provided by the user
         * in the create trigger sql. This sql is saved in the system
         * table. Since a column has been dropped from the trigger table,
         * the trigger action sql may not be valid anymore. To establish
         * that, we need to regenerate the internal representation of that
         * sql and bind it again.
         */
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        dd.dropTriggerDescriptor(trd, tc);

        // Here we get the trigger action sql and use the parser to build
        // the parse tree for it.
        SchemaDescriptor compSchema = dd.getSchemaDescriptor(
                dd.getSPSDescriptor(spsUUID).getCompSchemaId(),
                null);
        CompilerContext newCC = lcc.pushCompilerContext(compSchema);
        Parser    pa = newCC.getParser();
        String originalSQL = isWhenClause ? trd.getWhenClauseText()
                                          : trd.getTriggerDefinition(index);

        Visitable node = isWhenClause ? pa.parseSearchCondition(originalSQL)
                                      : pa.parseStatement(originalSQL);
        lcc.popCompilerContext(newCC);
        // Do not delete following. We use this in finally clause to
        // determine if the CompilerContext needs to be popped.
        newCC = null;

        try {
            // Regenerate the internal representation for the trigger action
            // sql using the ColumnReference classes in the parse tree. It
            // will catch dropped column getting used in trigger action sql
            // through the REFERENCING clause(this can happen only for the
            // the triggers created prior to 10.7. Trigger created with
            // 10.7 and higher keep track of trigger action column used
            // through the REFERENCING clause in system table and hence
            // use of dropped column will be detected earlier in this
            // method for such triggers).
            //
            // We might catch errors like following during this step.
            // Say that following pre-10.7 trigger exists in the system and
            // user is dropping column c11. During the regeneration of the
            // internal trigger action sql format, we will catch that
            // column oldt.c11 does not exist anymore
            // CREATE TRIGGER DERBY4998_SOFT_UPGRADE_RESTRICT_tr1
            //    AFTER UPDATE OF c12
            //    ON DERBY4998_SOFT_UPGRADE_RESTRICT REFERENCING OLD AS oldt
            //    FOR EACH ROW
            //    SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_RESTRICT

            SPSDescriptor sps = isWhenClause ? trd.getWhenClauseSPS(lcc, null)
                                             : trd.getActionSPS(lcc, index, null);
            int[] referencedColsInTriggerAction = new int[td.getNumberOfColumns()];
            java.util.Arrays.fill(referencedColsInTriggerAction, -1);
            String newText = dd.getTriggerActionString(node,
                trd.getOldReferencingName(),
                trd.getNewReferencingName(),
                originalSQL,
                trd.getReferencedCols(),
                referencedColsInTriggerAction,
                0,
                trd.getTableDescriptor(),
                trd.getTriggerEventDML(),
                true,
                null,
                null);

            if (isWhenClause) {
                // The WHEN clause is not a full SQL statement, just a search
                // condition, so we need to turn it into a statement in order
                // to create an SPS.
                newText = "VALUES ( " + newText + " )";
            }

            sps.setText(newText);

            // Now that we have the internal format of the trigger action sql,
            // bind that sql to make sure that we are not using colunm being
            // dropped in the trigger action sql directly (ie not through
            // REFERENCING clause.
            // eg
            // create table atdc_12 (a integer, b integer);
            // create trigger atdc_12_trigger_1 after update of a
            //     on atdc_12 for each row select a,b from atdc_12
            // Drop one of the columns used in the trigger action
            //   alter table atdc_12 drop column b
            // Following rebinding of the trigger action sql will catch the use
            // of column b in trigger atdc_12_trigger_1
            newCC = lcc.pushCompilerContext(compSchema);
            newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
            pa = newCC.getParser();
            StatementNode stmtnode = (StatementNode) pa.parseStatement(newText);
            // need a current dependent for bind
            newCC.setCurrentDependent(sps.getPreparedStatement());
            stmtnode.bindStatement();

        } catch (StandardException se) {
            /*
             *Need to catch for few different kinds of sql states depending
             * on what kind of trigger action sql is using the column being
             * dropped. Following are examples for different sql states
             *
             *SQLState.LANG_COLUMN_NOT_FOUND is thrown for following usage in
             * trigger action sql of column being dropped atdc_12.b
             *        create trigger atdc_12_trigger_1 after update
             *           of a on atdc_12
             *           for each row
             *           select a,b from atdc_12
             *
             *SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE is thrown for following
             * usage in trigger action sql of column being dropped
             * atdc_14_tab2a2 with restrict clause
             *        create trigger atdc_14_trigger_1 after update
             *           on atdc_14_tab1 REFERENCING NEW AS newt
             *           for each row
             *           update atdc_14_tab2 set a2 = newt.a1
             *
             * SQLState.LANG_DB2_INVALID_COLS_SPECIFIED is thrown for following
             *  usage in trigger action sql of column being dropped
             *  ATDC_13_TAB1_BACKUP.c11 with restrict clause
             *         create trigger ATDC_13_TAB1_trigger_1 after update
             *           on ATDC_13_TAB1 for each row
             *           INSERT INTO ATDC_13_TAB1_BACKUP
             *           SELECT C31, C32 from ATDC_13_TAB3
             *
             *SQLState.LANG_TABLE_NOT_FOUND is thrown for following scenario
             *   create view ATDC_13_VIEW2 as select c12 from ATDC_13_TAB3 where c12>0
             *Has following trigger defined
             *         create trigger ATDC_13_TAB1_trigger_3 after update
             *           on ATDC_13_TAB1 for each row
             *           SELECT * from ATDC_13_VIEW2
             * Ane drop column ATDC_13_TAB3.c12 is issued
             */
            if (se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND)||
                    (se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE) ||
                            (se.getMessageId().equals(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED) ||
                                    (se.getMessageId().equals(SQLState.LANG_TABLE_NOT_FOUND))))) {
                if (cascade) {
                    trd.drop(lcc);
                    activation.addWarning(
                            StandardException.newWarning(
                                    SQLState.LANG_TRIGGER_DROPPED,
                                    trd.getName(), td.getName()));
                    return true;
                }
                else
                {    // we'd better give an error if don't drop it,
                    throw StandardException.newException(
                            SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                            dd.getDependencyManager().getActionString(DependencyManager.DROP_COLUMN),
                            columnName, "TRIGGER",
                            trd.getName());
                }
            } else
                throw se;
        } finally {
            if (newCC != null)
                lcc.popCompilerContext(newCC);
        }

        /*
         * If we are here, then it means that the column being dropped
         * is not getting used in the trigger action.
         *
         * We have recreated the trigger action SPS and recollected the
         * column positions for trigger columns and trigger action columns
         * getting accessed through REFERENCING clause because
         * drop column can affect the column positioning of existing
         * columns in the table. We will save that in the system table.
        */
        dd.addDescriptor(trd, sd, DataDictionary.SYSTRIGGERS_CATALOG_NUM, false, tc, false);
        return false;
    }

    /**
     * Iterate through the received list of CreateIndexConstantActions and
     * execute each one, It's possible that one or more of the constant
     * actions in the list has been rendered "unneeded" by the time we get
     * here (because the index that the constant action was going to create
     * is no longer needed), so we have to check for that.
     *
     * @param newConglomActions Potentially empty list of constant actions
     *   to execute, if still needed
     * @param ixCongNums Optional array of conglomerate numbers; if non-null
     *   then any entries in the array which correspond to a dropped physical
     *   conglomerate (as determined from the list of constant actions) will
     *   be updated to have the conglomerate number of the newly-created
     *   physical conglomerate.
     */
    private void createNewBackingCongloms(Activation activation,
                                          TableDescriptor td,
                                          List<ConstantAction> newConglomActions,
                                          long [] ixCongNums) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        for (ConstantAction newConglomAction : newConglomActions) {
            CreateIndexConstantOperation ca = (CreateIndexConstantOperation) newConglomAction;

            if (dd.getConglomerateDescriptor(ca.getCreatedUUID()) == null) {
                /* Conglomerate descriptor was dropped after
                 * being selected as the source for a new
                 * conglomerate, so don't create the new
                 * conglomerate after all.  Either we found
                 * another conglomerate descriptor that can
                 * serve as the source for the new conglom,
                 * or else we don't need a new conglomerate
                 * at all because all constraints/indexes
                 * which shared it had a dependency on the
                 * dropped column and no longer exist.
                 */
                continue;
            }

            executeConglomReplacement(ca, activation);
            long oldCongNum = ca.getReplacedConglomNumber();
            long newCongNum = ca.getCreatedConglomNumber();

            /* The preceding call to executeConglomReplacement updated all
             * relevant ConglomerateDescriptors with the new conglomerate
             * number *WITHIN THE DATA DICTIONARY*.  But the table
             * descriptor that we have will not have been updated.
             * There are two approaches to syncing the table descriptor
             * with the dictionary: 1) refetch the table descriptor from
             * the dictionary, or 2) update the table descriptor directly.
             * We choose option #2 because the caller of this method (esp.
             * getAffectedIndexes()) has pointers to objects from the
             * table descriptor as it was before we entered this method.
             * It then changes data within those objects, with the
             * expectation that, later, those objects can be used to
             * persist the changes to disk.  If we change the table
             * descriptor here, the objects that will get persisted to
             * disk (from the table descriptor) will *not* be the same
             * as the objects that were updated--so we'll lose the updates
             * and that will in turn cause other problems.  So we go with
             * option #2 and just change the existing TableDescriptor to
             * reflect the fact that the conglomerate number has changed.
             */
            ConglomerateDescriptor[] tdCDs = td.getConglomerateDescriptors(oldCongNum);

            for (ConglomerateDescriptor tdCD : tdCDs)
                tdCD.setConglomerateNumber(newCongNum);

            /* If we received a list of index conglomerate numbers
             * then they are the "old" numbers; see if any of those
             * numbers should now be updated to reflect the new
             * conglomerate, and if so, update them.
             */
            if (ixCongNums != null) {
                for (int j = 0; j < ixCongNums.length; j++) {
                    if (ixCongNums[j] == oldCongNum)
                        ixCongNums[j] = newCongNum;
                }
            }
        }
    }

    /**
     * Return the "semi" row count of a table.  We are only interested in
     * whether the table has 0, 1 or > 1 rows.
     *
     *
     * @return Number of rows (0, 1 or > 1) in table.
     *
     * @exception StandardException        Thrown on failure
     */
    public static int getSemiRowCount(TransactionController tc, TableDescriptor td) throws StandardException {
        int numRows = 0;

        ScanController sc = tc.openScan(td.getHeapConglomerateId(),
                                        false,    // hold
                                        0,        // open read only
                                        TransactionController.MODE_TABLE,
                                        TransactionController.ISOLATION_SERIALIZABLE,
                                        RowUtil.EMPTY_ROW_BITSET, // scanColumnList
                                        null,    // start position
                                        ScanController.GE,      // startSearchOperation
                                        null, // scanQualifier
                                        null, //stop position - through last row
                                        ScanController.GT);     // stopSearchOperation

        while (sc.next()) {
            numRows++;
            // We're only interested in whether the table has 0, 1 or > 1 rows
            if (numRows == 2) {
                break;
            }
        }
        sc.close();

        return numRows;
    }

}
