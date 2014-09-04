package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.io.Closeables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.AlterTable.DropColumnJob;
import com.splicemachine.derby.impl.job.AlterTable.LoadConglomerateJob;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.JobFuture;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.uuid.Snowflake;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.sql.compile.CollectNodesVisitor;
import org.apache.derby.impl.sql.compile.ColumnDefinitionNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 9/3/14
 */
public class ModifyColumnConstantOperation extends AlterTableConstantOperation{

    /**
     * Make the AlterAction for an ALTER TABLE statement.
     *
     * @param sd                     descriptor for the table's schema.
     * @param tableName              Name of table.
     * @param tableId                UUID of table
     * @param tableConglomerateId    heap conglomerate number of table
     * @param columnInfo             Information on all the columns in the table.
     * @param constraintActions      ConstraintConstantAction[] for constraints
     * @param lockGranularity        The lock granularity.
     * @param behavior               drop behavior for dropping column
     * @param indexNameForStatistics Will name the index whose statistics
     */
    public ModifyColumnConstantOperation(SchemaDescriptor sd, String tableName, UUID tableId,
                                         long tableConglomerateId,
                                         ColumnInfo[] columnInfo, ConstantAction[] constraintActions,
                                         char lockGranularity, int behavior,
                                         String indexNameForStatistics) {
        super(sd, tableName, tableId, tableConglomerateId,
                columnInfo, constraintActions,
                lockGranularity, behavior, indexNameForStatistics);
    }

    @Override
    protected void executeConstantActionBody(Activation activation) throws StandardException {

        // Save references to the main structures we need.

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
		    /*
				** Inform the data dictionary that we are about to write to it.
				** There are several calls to data dictionary "get" methods here
				** that might be done in "read" mode in the data dictionary, but
				** it seemed safer to do this whole operation in "write" mode.
				**
				** We tell the data dictionary we're done writing at the end of
				** the transaction.
				*/
        dd.startWriting(lcc);

        // now do the real work

        // get an exclusive lock of the heap, to avoid deadlock on rows of
        // SYSCOLUMNS etc datadictionary tables and phantom table
        // descriptor, in which case table shape could be changed by a
        // concurrent thread doing add/drop column.

        // older version (or at target) has to get td first, potential deadlock
        TableDescriptor td = getTableDescriptor(dd);

        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        // Save the TableDescriptor off in the Activation
        activation.setDDLTableDescriptor(td);

		   /*
		    ** If the schema descriptor is null, then we must have just read
        ** ourselves in.  So we will get the corresponding schema descriptor
        ** from the data dictionary.
		    */
        if (sd == null) {
            sd = getAndCheckSchemaDescriptor(dd, schemaId, "ALTER TABLE");
        }

		    /* Prepare all dependents to invalidate.  (This is their chance
		     * to say that they can't be invalidated.  For example, an open
		     * cursor referencing a table/view that the user is attempting to
		     * alter.) If no one objects, then invalidate any dependent objects.
		     */
        //TODO -sf- do we need to invalidate twice?
        dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        int numRows = manageColumnInfo(activation,td);
        // adjust dependencies on user defined types
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );

        executeConstraintActions(activation, td,numRows);
        adjustLockGranularity();
    }

    private int manageColumnInfo(Activation activation,TableDescriptor td) throws StandardException{
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        TransactionController tc = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
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
            if ((aColumnInfo.action == ColumnInfo.CREATE) && !(aColumnInfo.dataType.isNullable()) &&
                    (aColumnInfo.defaultInfo == null) && (aColumnInfo.autoincInc == 0)) {
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
                    addNewColumnToTable(lcc,dd,td,tc,ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT:
                    modifyColumnDefault(lcc,dd,td,ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_TYPE:
                    modifyColumnType(dd,tc,td,ix);
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
                    dropColumnFromTable(activation,td,columnInfo[ix].name);
                    break;
                default:
                    SanityManager.THROWASSERT("Unexpected action in AlterTableConstantAction");
            }
        }
        return numRows;
    }

    /*private helper methods*/
    private void addNewColumnToTable(LanguageConnectionContext lcc,
                                     DataDictionary dd,
                                     TableDescriptor td,
                                     TransactionController tc,
                                     int infoIndex) throws StandardException {
        ColumnInfo colInfo = columnInfo[infoIndex];
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(colInfo.name);
        DataValueDescriptor storableDV;
        int colNumber = td.getMaxColumnID() + infoIndex;

		    /* We need to verify that the table does not have an existing
		     * column with the same name before we try to add the new
		     * one as addColumnDescriptor() is a void method.
		     */
        if (columnDescriptor != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,columnDescriptor.getDescriptorType(),
                    colInfo.name,td.getDescriptorType(),td.getQualifiedName());
        }

        if (colInfo.defaultValue != null)
            storableDV = colInfo.defaultValue;
        else
            storableDV = colInfo.dataType.getNull();

        // Add the column to the conglomerate.(Column ids in store are 0-based)
        tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV,  colInfo.dataType.getCollationType());

        UUID defaultUUID = colInfo.newDefaultUUID;

		    /* Generate a UUID for the default, if one exists
		     * and there is no default id yet.
		     */
        if (colInfo.defaultInfo != null && defaultUUID == null) {
            defaultUUID = dd.getUUIDFactory().createUUID();
        }

        // Add the column to syscolumns.
        // Column ids in system tables are 1-based
        columnDescriptor =
                new ColumnDescriptor(
                        colInfo.name,
                        colNumber + 1,
                        colInfo.dataType,
                        colInfo.defaultValue,
                        colInfo.defaultInfo,
                        td,
                        defaultUUID,
                        colInfo.autoincStart,
                        colInfo.autoincInc
                );

        dd.addDescriptor(columnDescriptor, td,DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        //notify other servers of the change
        DDLChange change = new DDLChange(((SpliceTransactionManager)tc).getActiveStateTxn(), DDLChange.TentativeType.ADD_COLUMN);
        notifyMetadataChange(change);

        // now add the column to the tables column descriptor list.
        //noinspection unchecked
        td.getColumnDescriptorList().add(columnDescriptor);

        if (columnDescriptor.isAutoincrement()) {
            updateNewAutoincrementColumn(lcc,td,colInfo);
        }

        // Update the new column to its default, if it has a non-null default
        if (columnDescriptor.hasNonNullDefault()) {
            updateNewColumnToDefault(lcc,td,columnDescriptor);
        }

        //
        // Add dependencies. These can arise if a generated column depends
        // on a user created function.
        //
        addColumnDependencies( lcc, dd, td, colInfo);

        // Update SYSCOLPERMS table which tracks the permissions granted
        // at columns level. The sytem table has a bit map of all the columns
        // in the user table to help determine which columns have the
        // permission granted on them. Since we are adding a new column,
        // that bit map needs to be expanded and initialize the bit for it
        // to 0 since at the time of ADD COLUMN, no permissions have been
        // granted on that new column.
        //
        dd.updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
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
        //							schemaName, tableName,
        //							columnName)
        String updateStmt = "UPDATE " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
                " SET " + IdUtil.normalToDelimited(columnName) + "=" +
                "org.apache.derby.iapi.db.ConnectionInfo::" +
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

    private void updateNewColumnToDefault(LanguageConnectionContext lcc,TableDescriptor td,ColumnDescriptor columnDescriptor) throws StandardException {
        /*
         * Update a new column with its default.
         * We could do the scan ourself here, but
         * instead we get a nested connection and
         * issue the appropriate update statement.
         *
         * @param columnDescriptor  catalog descriptor for the column
         */
        DefaultInfo defaultInfo = columnDescriptor.getDefaultInfo();
        String  columnName = columnDescriptor.getColumnName();
        String  defaultText;

        if ( defaultInfo.isGeneratedColumn() ) { defaultText = "default"; }
        else { defaultText = columnDescriptor.getDefaultInfo().getDefaultText(); }

		    /* Need to use delimited identifiers for all object names
		     * to ensure correctness.
		     */
        String updateStmt = "UPDATE " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
                " SET " + IdUtil.normalToDelimited(columnName) + "=" +
                defaultText;

        AlterTableConstantOperation.executeUpdate(lcc, updateStmt);
    }

    private void modifyColumnDefault(LanguageConnectionContext lcc, DataDictionary dd,TableDescriptor td,int ix) throws StandardException {
        ColumnInfo colInfo = columnInfo[ix];
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(colInfo.name);
        int columnPosition = columnDescriptor.getPosition();

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
                colInfo.dataType,
                colInfo.defaultValue,
                colInfo.defaultInfo,
                td,
                defaultUUID,
                colInfo.autoincStart,
                colInfo.autoincInc,
                colInfo.autoinc_create_or_modify_Start_Increment
        );

        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), colInfo.name, tc);
        dd.addDescriptor(columnDescriptor, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        if (colInfo.action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT) {
            // adding an autoincrement default-- calculate the maximum value
            // of the autoincrement column.
            long maxValue = getColumnMax(lcc,td, colInfo.name, colInfo.autoincInc);
            dd.setAutoincrementValue(tc, td.getUUID(), colInfo.name, maxValue, true);
        } else if (colInfo.action == ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART) {
            dd.setAutoincrementValue(tc, td.getUUID(), colInfo.name, colInfo.autoincStart, false);
        }
    }

    private void modifyColumnType(DataDictionary dd, TransactionController tc,TableDescriptor td,int ix) throws StandardException {
        ColumnDescriptor columnDescriptor =
                td.getColumnDescriptor(columnInfo[ix].name),
                newColumnDescriptor;

        newColumnDescriptor =
                new ColumnDescriptor(columnInfo[ix].name,
                        columnDescriptor.getPosition(),
                        columnInfo[ix].dataType,
                        columnDescriptor.getDefaultValue(),
                        columnDescriptor.getDefaultInfo(),
                        td,
                        columnDescriptor.getDefaultUUID(),
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc
                );



        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
        dd.addDescriptor(newColumnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
    }

    private boolean validateNotNullConstraint (
            String							columnNames[],
            boolean							nullCols[],
            int								numRows,
            LanguageConnectionContext		lcc,
            TableDescriptor td,
            String							errorMsg ) throws StandardException {
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
                if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT)) {	//alter table add primary key
                    //soft upgrade mode
                    throw ErrorState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT.newException(td.getQualifiedName());
                } else if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY)) {	//alter table add primary key
                    throw ErrorState.LANG_NULL_DATA_IN_PRIMARY_KEY.newException(td.getQualifiedName());
                } else {	//alter table modify column not null
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

        newColumnDescriptor = new ColumnDescriptor(colName,
                        columnDescriptor.getPosition(),
                        dataType,
                        columnDescriptor.getDefaultValue(),
                        columnDescriptor.getDefaultInfo(),
                        td,
                        columnDescriptor.getDefaultUUID(),
                        columnDescriptor.getAutoincStart(),
                        columnDescriptor.getAutoincInc());

        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), colName, tc);
        dd.addDescriptor(newColumnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
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
     *						statement-- currently we allow only one.
     * @exception StandardException 	thrown on failure.
     */
    @SuppressWarnings("unchecked")
    private void dropColumnFromTable(Activation activation,TableDescriptor td,String columnName ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        boolean cascade = (behavior == StatementType.DROP_CASCADE);
        // drop any generated columns which reference this column
        ColumnDescriptorList generatedColumnList = td.getGeneratedColumns();
        int generatedColumnCount = generatedColumnList.size();
        List<String> cascadedDroppedColumns = new ArrayList<String>(generatedColumnCount);
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

        int cascadedDrops = cascadedDroppedColumns.size();
        int sizeAfterCascadedDrops = td.getColumnDescriptorList().size() - cascadedDrops;

        // can NOT drop a column if it is the only one in the table
        if (sizeAfterCascadedDrops == 1) {
            throw ErrorState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.newException(
                    dm.getActionString(DependencyManager.DROP_COLUMN),
                    "THE *LAST* COLUMN " + columnName,"TABLE",td.getQualifiedName());
        }

        // now drop dependent generated columns
        for (String generatedColumnName : cascadedDroppedColumns) {
            activation.addWarning(StandardException.newWarning(SQLState.LANG_GEN_COL_DROPPED, generatedColumnName, td.getName()));

            //
            // We can only recurse 2 levels since a generation clause cannot
            // refer to other generated columns.
            //
            dropColumnFromTable(activation,td,generatedColumnName);
        }

        /*
         * Cascaded drops of dependent generated columns may require us to
         * rebuild the table descriptor.
         */
        td = dd.getTableDescriptor(tableId);
        TransactionController tc = lcc.getTransactionExecute();

        ColumnDescriptor columnDescriptor = td.getColumnDescriptor( columnName );

        // We already verified this in bind, but do it again
        if (columnDescriptor == null) {
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName,td.getQualifiedName());
        }

        int size = td.getColumnDescriptorList().size();
        int droppedColumnPosition = columnDescriptor.getPosition();

        FormatableBitSet toDrop = new FormatableBitSet(size + 1);
        toDrop.set(droppedColumnPosition);
        td.setReferencedColumnMap(toDrop);

        dm.invalidateFor(td, (cascade ? DependencyManager.DROP_COLUMN: DependencyManager.DROP_COLUMN_RESTRICT),lcc);

        // If column has a default we drop the default and any dependencies
        if (columnDescriptor.getDefaultInfo() != null) {
            dm.clearDependencies(lcc, columnDescriptor.getDefaultDescriptor(dd));
        }

        //Now go through each trigger on this table and see if the column
        //being dropped is part of it's trigger columns or trigger action
        //columns which are used through REFERENCING clause
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
                                    dm.getActionString(DependencyManager.DROP_COLUMN),
                                    columnName, "TRIGGER",
                                    trd.getName());
                        }
                        break;
                    }
                }

                // The following if condition will be true if the column
                // getting dropped is not a trigger column, but one or more
                // of the trigge column's position has changed because of
                // drop column.
                if (j == refColLen && changed) {
                    dd.dropTriggerDescriptor(trd, tc);
                    for (j = 0; j < refColLen; j++) {
                        if (referencedCols[j] > droppedColumnPosition)
                            referencedCols[j]--;
                    }
                    dd.addDescriptor(trd, sd,
                            DataDictionary.SYSTRIGGERS_CATALOG_NUM,
                            false, tc);
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
                                    dm.getActionString(DependencyManager.DROP_COLUMN),
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
                            false, tc);
                }
            }
        }

        ConstraintDescriptorList csdl = dd.getConstraintDescriptors(td);
        int csdl_size = csdl.size();

        ArrayList newCongloms = new ArrayList();

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

		    /* If there are new backing conglomerates which must be
		     * created to replace a dropped shared conglomerate
		     * (where the shared conglomerate was dropped as part
		     * of a "drop constraint" call above), then create them
		     * now.  We do this *after* dropping all dependent
		     * constraints because we don't want to waste time
		     * creating a new conglomerate if it's just going to be
		     * dropped again as part of another "drop constraint".
		     */
        createNewBackingCongloms(activation,td,newCongloms, null);

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
        td = dd.getTableDescriptor(tableId);

        performColumnDrop(activation,td,tc,droppedColumnPosition);

        ColumnDescriptorList tab_cdl = td.getColumnDescriptorList();

        // drop the column from syscolumns
        dd.dropColumnDescriptor(td.getUUID(), columnName, tc);
        ColumnDescriptor[] cdlArray =
                new ColumnDescriptor[size - columnDescriptor.getPosition()];

        // For each column in this table with a higher column position,
        // drop the entry from SYSCOLUMNS, but hold on to the column
        // descriptor and reset its position to adjust for the dropped
        // column. Then, re-add all those adjusted column descriptors
        // back to SYSCOLUMNS
        //
        for (int i = columnDescriptor.getPosition(), j = 0; i < size; i++, j++) {
            ColumnDescriptor cd = tab_cdl.elementAt(i);
            dd.dropColumnDescriptor(td.getUUID(), cd.getColumnName(), tc);
            cd.setPosition(i);
            if (cd.isAutoincrement()) {
                cd.setAutoinc_create_or_modify_Start_Increment( ColumnDefinitionNode.CREATE_AUTOINCREMENT);
            }
            cdlArray[j] = cd;
        }
        dd.addDescriptorArray(cdlArray, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        /*
         * By this time, the column has been removed from the table descriptor.
         *  Now, go through all the triggers and regenerate their trigger action
         *  SPS and rebind the generated trigger action sql. If the trigger
         *  action is using the dropped column, it will get detected here. If
         *  not, then we will have generated the internal trigger action sql
         *  which matches the trigger action sql provided by the user.
         *
         *  eg of positive test case
         *  create table atdc_16_tab1 (a1 integer, b1 integer, c1 integer);
         *  create table atdc_16_tab2 (a2 integer, b2 integer, c2 integer);
         *  create trigger atdc_16_trigger_1
         *     after update of b1 on atdc_16_tab1
         *     REFERENCING NEW AS newt
         *     for each row
         *     update atdc_16_tab2 set c2 = newt.c1
         *  The internal representation for the trigger action before the column
         *  is dropped is as follows
         *  	 update atdc_16_tab2 set c2 =
         *    org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
         *    getONewRow().getInt(3)
         *  After the drop column shown as below
         *    alter table DERBY4998_SOFT_UPGRADE_RESTRICT drop column c11
         *  The above internal representation of tigger action sql is not
         *  correct anymore because column position of c1 in atdc_16_tab1 has
         *  now changed from 3 to 2. Following while loop will regenerate it and
         *  change it to as follows
         *  	 update atdc_16_tab2 set c2 =
         *    org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
         *    getONewRow().getInt(2)
         *
         *  We could not do this before the actual column drop, because the
         *  rebind would have still found the column being dropped in the
         *  table descriptor and hence use of such a column in the trigger
         *  action rebind would not have been caught.

         * For the table on which ALTER TABLE is getting performed, find out
         *  all the SPSDescriptors that use that table as a provider. We are
         *  looking for SPSDescriptors that have been created internally for
         *  trigger action SPSes. Through those SPSDescriptors, we will be
         *  able to get to the triggers dependent on the table being altered
         * Following will get all the dependent objects that are using
         *  ALTER TABLE table as provider
         */
        List depsOnAlterTableList = dd.getProvidersDescriptorList(td.getObjectID().toString());
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
                        columnDroppedAndTriggerDependencies(trdToBeDropped,td, cascade, columnName,activation);
                    }
                }
            }
        }
        // Adjust the column permissions rows in SYSCOLPERMS to reflect the
        // changed column positions due to the dropped column:
        dd.updateSYSCOLPERMSforDropColumn(td.getUUID(), tc, columnDescriptor);

        // remove column descriptor from table descriptor. this fixes up the
        // list in case we were called recursively in order to cascade-drop a
        // dependent generated column.
        tab_cdl.remove( td.getColumnDescriptor( columnName ) );
    }

    private void columnDroppedAndTriggerDependencies(TriggerDescriptor trd,
                                                     TableDescriptor td,
                                                     boolean cascade,
                                                     String columnName,
                                                     Activation activation)
            throws StandardException {
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
        SchemaDescriptor compSchema;
        compSchema = dd.getSchemaDescriptor(trd.getSchemaDescriptor().getUUID(), null);
        CompilerContext newCC = lcc.pushCompilerContext(compSchema);
        Parser pa = newCC.getParser();
        StatementNode stmtnode = (StatementNode)pa.parseStatement(trd.getTriggerDefinition());
        lcc.popCompilerContext(newCC);
        // Do not delete following. We use this in finally clause to
        // determine if the CompilerContext needs to be popped.
        newCC = null;

        try {
            // We are interested in ColumnReference classes in the parse tree
            CollectNodesVisitor visitor = new CollectNodesVisitor(ColumnReference.class);
            stmtnode.accept(visitor);

            /*
             * Regenerate the internal representation for the trigger action
             * sql using the ColumnReference classes in the parse tree. It
             * will catch dropped column getting used in trigger action sql
             * through the REFERENCING clause(this can happen only for the
             * the triggers created prior to 10.7. Trigger created with
             * 10.7 and higher keep track of trigger action column used
             * through the REFERENCING clause in system table and hence
             * use of dropped column will be detected earlier in this
             * method for such triggers).
             *
             * We might catch errors like following during this step.
             * Say that following pre-10.7 trigger exists in the system and
             * user is dropping column c11. During the regeneration of the
             * internal trigger action sql format, we will catch that
             * column oldt.c11 does not exist anymore
             * CREATE TRIGGER DERBY4998_SOFT_UPGRADE_RESTRICT_tr1
             *    AFTER UPDATE OF c12
             *    ON DERBY4998_SOFT_UPGRADE_RESTRICT REFERENCING OLD AS oldt
             *    FOR EACH ROW
             *    SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_RESTRICT
             */
            SPSDescriptor triggerActionSPSD = trd.getActionSPS(lcc);
            int[] referencedColsInTriggerAction = new int[td.getNumberOfColumns()];
            java.util.Arrays.fill(referencedColsInTriggerAction, -1);
            triggerActionSPSD.setText(dd.getTriggerActionString(stmtnode,
                    trd.getOldReferencingName(),
                    trd.getNewReferencingName(),
                    trd.getTriggerDefinition(),
                    trd.getReferencedCols(),
                    referencedColsInTriggerAction,
                    0,
                    trd.getTableDescriptor(),
                    trd.getTriggerEventMask(),
                    true
            ));

            /*
             * Now that we have the internal format of the trigger action sql,
             * bind that sql to make sure that we are not using colunm being
             * dropped in the trigger action sql directly (ie not through
             * REFERENCING clause.
             * eg
             * create table atdc_12 (a integer, b integer);
             * create trigger atdc_12_trigger_1 after update of a
             *     on atdc_12 for each row select a,b from atdc_12
             * Drop one of the columns used in the trigger action
             *   alter table atdc_12 drop column b
             * Following rebinding of the trigger action sql will catch the use
             * of column b in trigger atdc_12_trigger_1
             */
            compSchema = dd.getSchemaDescriptor(trd.getSchemaDescriptor().getUUID(), null);
            newCC = lcc.pushCompilerContext(compSchema);
            newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
            pa = newCC.getParser();
            stmtnode = (StatementNode)pa.parseStatement(triggerActionSPSD.getText());
            // need a current dependent for bind
            newCC.setCurrentDependent(triggerActionSPSD.getPreparedStatement());
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
                    return;
                }
                else
                {	// we'd better give an error if don't drop it,
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
        dd.addDescriptor(trd, sd, DataDictionary.SYSTRIGGERS_CATALOG_NUM, false, tc);
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
    private void createNewBackingCongloms(Activation activation, TableDescriptor td,ArrayList newConglomActions, long [] ixCongNums) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        for (Object newConglomAction : newConglomActions) {
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
						 * descriptor here the objects that will get persisted to
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

    private void performColumnDrop(Activation activation,
                                   TableDescriptor td,
                                   TransactionController parent,int droppedColumnPosition) throws StandardException {
        long newHeapConglom;
        Properties properties = new Properties();

        ExecRow emptyHeapRow  = td.getEmptyExecRow();
        int[]   collation_ids = td.getColumnCollationIds();

        ConglomerateController compressHeapCC =
                parent.openConglomerate(
                        td.getHeapConglomerateId(),
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);


        // Get the properties on the old heap
        compressHeapCC.getInternalTablePropertySet(properties);
        compressHeapCC.close();

        // Create an array to put base row template
//        ExecRow baseRow = new ExecRow[bulkFetchSize];
//        DataValueDescriptor[][] baseRowArray = new DataValueDescriptor[bulkFetchSize][];
//        boolean[] validRow = new boolean[bulkFetchSize];

		    /* Set up index info */
        getAffectedIndexes(td);

        // Get an array of RowLocation template
//        RowLocation compressRL = new RowLocation[bulkFetchSize];
//        ExecIndexRow indexRows  = new ExecIndexRow[numIndexes];
        // must be a drop column, thus the number of columns in the
        // new template row and the collation template is one less.
        ExecRow newRow = activation.getExecutionFactory().getValueRow(emptyHeapRow.nColumns() - 1);

        int[] new_collation_ids = new int[collation_ids.length - 1];

        for (int i = 0; i < newRow.nColumns(); i++) {
            newRow.setColumn(
                    i + 1,
                    i < droppedColumnPosition - 1 ?
                            emptyHeapRow.getColumn(i + 1) :
                            emptyHeapRow.getColumn(i + 1 + 1));

            new_collation_ids[i] =
                    collation_ids[
                            (i < droppedColumnPosition - 1) ? i : (i + 1)];
        }

        emptyHeapRow = newRow;
        collation_ids = new_collation_ids;

        // calculate column order for new table
        TxnView parentTxn = ((SpliceTransactionManager)parent).getActiveStateTxn();
        int[] oldColumnOrder = DataDictionaryUtils.getColumnOrdering(parentTxn, tableId);
        int[] newColumnOrder = DataDictionaryUtils.getColumnOrderingAfterDropColumn(oldColumnOrder, droppedColumnPosition);
        IndexColumnOrder[] columnOrdering = null;
        if (newColumnOrder != null && newColumnOrder.length > 0) {
            columnOrdering = new IndexColumnOrder[newColumnOrder.length];
            for (int i = 0; i < newColumnOrder.length; ++i) {
                columnOrdering[i] = new IndexColumnOrder(newColumnOrder[i]);
            }
        }
        // Create a new table -- use createConglomerate() to avoid confusing calls to createAndLoad()
        newHeapConglom = parent.createConglomerate("heap", emptyHeapRow.getRowArray(),
                columnOrdering, collation_ids,
                properties, TransactionController.IS_DEFAULT);


        // Start a tentative txn to notify DDL('alter table drop column') change
        final long tableConglomId = td.getHeapConglomerateId();
        Txn tentativeTransaction;
        try {
            tentativeTransaction = TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn,Long.toString(tableConglomId).getBytes());
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        ColumnInfo[] allColumnInfo = DataDictionaryUtils.getColumnInfo(td);
        TentativeDropColumnDesc tentativeDropColumnDesc =
                new TentativeDropColumnDesc( tableId, newHeapConglom, td.getHeapConglomerateId(), allColumnInfo, droppedColumnPosition);

        DDLChange ddlChange = new DDLChange(tentativeTransaction, DDLChange.TentativeType.DROP_COLUMN);
        ddlChange.setTentativeDDLDesc(tentativeDropColumnDesc);
        ddlChange.setParentTxn(parentTxn);

        notifyMetadataChange(ddlChange);

        HTableInterface table = SpliceAccessManager.getHTable(Long.toString(tableConglomId).getBytes());
        try {
            //Add a handler to drop a column to all regions
            tentativeDropColumn(table, newHeapConglom, tableConglomId, ddlChange);

            //wait for all past txns to complete
            Txn dropColumnTransaction = getDropColumnTransaction(parentTxn, tentativeTransaction,  tableConglomId);

            // Copy data from old table to the new table
            //copyToConglomerate(newHeapConglom, tentativeTransaction.getTransactionIdString(), co);
            copyToConglomerate(activation,td,droppedColumnPosition,newHeapConglom, parentTxn,tentativeTransaction.getCommitTimestamp());
            dropColumnTransaction.commit();
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        } finally{
            Closeables.closeQuietly(table);
        }

        // Set the "estimated" row count
//        ScanController compressHeapSC = tc.openScan(
//                newHeapConglom,
//                false,
//                TransactionController.OPENMODE_FORUPDATE,
//                TransactionController.MODE_TABLE,
//                TransactionController.ISOLATION_SERIALIZABLE,
//                null,
//                null,
//                0,
//                null,
//                null,
//                0);
//
//        compressHeapSC.setEstimatedRowCount(rowCount);
//
//        compressHeapSC.close();

				/*
				** Inform the data dictionary that we are about to write to it.
				** There are several calls to data dictionary "get" methods here
				** that might be done in "read" mode in the data dictionary, but
				** it seemed safer to do this whole operation in "write" mode.
				**
				** We tell the data dictionary we're done writing at the end of
				** the transaction.
				*/
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        dd.startWriting(lcc);

        // Update all indexes
        if (compressIRGs.length > 0) {
            updateAllIndexes(newHeapConglom, dd,td,tc);
        }

				/* Update the DataDictionary
				 * RESOLVE - this will change in 1.4 because we will get
				 * back the same conglomerate number
				 */
        // Get the ConglomerateDescriptor for the heap
        long oldHeapConglom = td.getHeapConglomerateId();
        ConglomerateDescriptor cd =
                td.getConglomerateDescriptor(oldHeapConglom);

        // Update sys.sysconglomerates with new conglomerate #
        dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dd.getDependencyManager().invalidateFor(td, DependencyManager.COMPRESS_TABLE, lcc);

        cleanUp();
    }

    private void copyToConglomerate(Activation activation,
                                    TableDescriptor td,
                                    int droppedColumnPosition,
                                    long toConglomId,
                                    TxnView dropColumnTxn,
                                    long demarcationPoint) throws StandardException {

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        String user = lcc.getSessionUserId();
        Snowflake snowflake = SpliceDriver.driver().getUUIDGenerator();
        long sId = snowflake.nextUUID();
        if (activation.isTraced()) {
            activation.getLanguageConnectionContext().setXplainStatementId(sId);
        }
        StatementInfo statementInfo =
                new StatementInfo(String.format("alter table %s.%s drop %s", td.getSchemaName(), td.getName(), columnInfo[0].name),
                        user,dropColumnTxn, 1, sId);
        OperationInfo opInfo = new OperationInfo(
                SpliceDriver.driver().getUUIDGenerator().nextUUID(), statementInfo.getStatementUuid(),"Alter Table Drop Column", null, false, -1l);
        statementInfo.setOperationInfo(Arrays.asList(opInfo));
        SpliceDriver.driver().getStatementManager().addStatementInfo(statementInfo);

        JobFuture future;
        JobInfo info;
        try{
            long fromConglomId = td.getHeapConglomerateId();
            HTableInterface table = SpliceAccessManager.getHTable(Long.toString(fromConglomId).getBytes());

            ColumnInfo[] allColumnInfo = DataDictionaryUtils.getColumnInfo(td);
            LoadConglomerateJob job = new LoadConglomerateJob(table,
                    tableId, fromConglomId, toConglomId, allColumnInfo, droppedColumnPosition, dropColumnTxn,
                    statementInfo.getStatementUuid(), opInfo.getOperationUuid(),demarcationPoint, activation.isTraced());
            long start = System.currentTimeMillis();
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(),future.getNumTasks(),start);
            info.setJobFuture(future);
            statementInfo.addRunningJob(opInfo.getOperationUuid(),info);
            try{
                future.completeAll(info);
            }catch(ExecutionException e){
                info.failJob();
                throw e;
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }
            statementInfo.completeJob(info);

        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        }finally {
            SpliceDriver.driver().getStatementManager().completedStatement(statementInfo, activation.isTraced());

        }

    }

    private void tentativeDropColumn(HTableInterface table,
                                     long newConglomId,
                                     long oldConglomId,
                                     DDLChange ddlChange) throws StandardException{
        JobFuture future = null;
        JobInfo info = null;
        try{
            long start = System.currentTimeMillis();
            DropColumnJob job = new DropColumnJob(table, oldConglomId, newConglomId, ddlChange);
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(),future.getNumTasks(),start);
            info.setJobFuture(future);
            try{
                future.completeAll(info); //TODO -sf- add status information
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }catch(Throwable t){
                info.failJob();
                throw t;
            }
            //statementInfo.completeJob(info);
        }catch (Throwable e) {
            if(info!=null) info.failJob();
            Closeables.closeQuietly(table);
            throw Exceptions.parseException(e);
        }finally {
            cleanupFuture(future);
        }
    }

    private Txn getDropColumnTransaction(TxnView parentTransaction,
                                         Txn tentativeTransaction,
                                         long tableConglomId) throws StandardException {
        Txn waitTxn;
        try {
            waitTxn = TransactionLifecycle.getLifecycleManager().chainTransaction(
                    parentTransaction,
                    Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                    false, Bytes.toBytes(Long.toString(tableConglomId)),tentativeTransaction);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        // Wait for past transactions to die
        List<TxnView> toIgnore = Arrays.asList(parentTransaction, waitTxn,tentativeTransaction);
        long activeTxnId;
        try {
            activeTxnId = waitForConcurrentTransactions(waitTxn, toIgnore,tableConglomId);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if (activeTxnId>=0) {
            throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("DropColumn",activeTxnId);
        }
        Txn dropColumnTransaction;
        try {
            dropColumnTransaction = TransactionLifecycle.getLifecycleManager().chainTransaction(
                    parentTransaction,
                    Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                    false, Bytes.toBytes(Long.toString(tableConglomId)),waitTxn);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        return dropColumnTransaction;
    }


    private void cleanupFuture(JobFuture future) throws StandardException {
        if (future!=null) {
            try {
                future.cleanup();
            } catch (ExecutionException e) {
                //noinspection ThrowFromFinallyBlock
                throw Exceptions.parseException(e.getCause());
            }
        }
    }
}
