package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.io.Closeables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.AlterTable.DropColumnJob;
import com.splicemachine.derby.impl.job.AlterTable.LoadConglomerateJob;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.primitives.BooleanArrays;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;
import org.apache.derby.catalog.*;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StreamStorable;
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
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.*;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.sql.compile.CollectNodesVisitor;
import org.apache.derby.impl.sql.compile.ColumnDefinitionNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.apache.derby.impl.sql.execute.BasicSortObserver;
import org.apache.derby.impl.sql.execute.CardinalityCounter;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 *	This class  describes actions that are ALWAYS performed for an
 *	ALTER TABLE Statement at Execution time.
 *
 */

public class AlterTableConstantOperation extends IndexConstantOperation implements RowLocationRetRowSource {
    private static final Logger LOG = Logger.getLogger(AlterTableConstantOperation.class);
    // copied from constructor args and stored locally.
    private	    SchemaDescriptor			sd;
    protected String						tableName;
    private	    UUID						schemaId;
    private	    ColumnInfo[]				columnInfo;
    private	    ConstraintConstantOperation[]	constraintActions;
    private	    char						lockGranularity;
    protected long						tableConglomerateId;
    private	    boolean					    compressTable;
    private     int						    behavior;
    private	    boolean					    sequential;
//    private     boolean                     truncateTable;
    private	    boolean					    purge;

    /**
     * updateStatistics will indicate that we are here for updating the
     * statistics. It could be statistics of just one index or all the
     * indexes on a given table.
     */
    private	    boolean					    updateStatistics;
    /**
     * The flag updateStatisticsAll will tell if we are going to update the
     * statistics of all indexes or just one index on a table.
     */
    private	    boolean					    updateStatisticsAll;
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
    private	    String						indexNameForStatistics;


    // Alter table compress and Drop column
    private     boolean					    doneScan;
    private     boolean[]				    needToDropSort;
    private     boolean[]				    validRow;
    private	    int						    bulkFetchSize = 16;
    private	    int						    currentCompressRow;
    private     int						    numIndexes;
    private     int						    rowCount;
    private     long					    estimatedRowCount;
    private     long[]					    indexConglomerateNumbers;
    private	    long[]					    sortIds;
    private     FormatableBitSet			indexedCols;
    private     ConglomerateController	    compressHeapCC;
    private     ExecIndexRow[]			    indexRows;
    private     ExecRow[]				    baseRow;
    private     ExecRow					    currentRow;
    private	    GroupFetchScanController    compressHeapGSC;
    protected IndexRowGenerator[]		    compressIRGs;
    private	    DataValueDescriptor[][]		baseRowArray;
    private     RowLocation[]			    compressRL;
    private     SortController[]		    sorters;
    private     int						    droppedColumnPosition;
    private     ColumnOrdering[][]		    ordering;
    private     int[][]		                collation;

    private	TableDescriptor 		        td;

    // CONSTRUCTORS
    private LanguageConnectionContext lcc;
    private DataDictionary dd;
    private DependencyManager dm;
    private TransactionController tc;
    private Activation activation;

    /**
     *	Make the AlterAction for an ALTER TABLE statement.
     *
     * @param sd              descriptor for the table's schema.
     *  @param tableName          Name of table.
     * @param tableId            UUID of table
     * @param tableConglomerateId  heap conglomerate number of table
     * @param columnInfo          Information on all the columns in the table.
     * @param constraintActions  ConstraintConstantAction[] for constraints
     * @param lockGranularity      The lock granularity.
     * @param compressTable      Whether or not this is a compress table
     * @param behavior            drop behavior for dropping column
     * @param sequential          If compress table/drop column,
*	                            whether or not sequential
     * @param purge        PURGE during INPLACE COMPRESS?
     * @param updateStatistics    TRUE means we are here to update statistics
     * @param updateStatisticsAll  TRUE means we are here to update statistics
*  	of all the indexes. False means we are here to update statistics of
*  	only one index.
     * @param dropStatistics    TRUE means we are here to drop statistics
     * @param dropStatisticsAll  TRUE means we are here to drop statistics
*  	of all the indexes. False means we are here to drop statistics of
*  	only one index.
     * @param indexNameForStatistics  Will name the index whose statistics
*  	will be updated/dropped. This param is looked at only if
*  	updateStatisticsAll/dropStatisticsAll is set to false and
     */
    public AlterTableConstantOperation(
            SchemaDescriptor sd,
            String tableName,
            UUID tableId,
            long tableConglomerateId,
            ColumnInfo[] columnInfo,
            ConstantAction[] constraintActions,
            char lockGranularity,
            boolean compressTable,
            int behavior,
            boolean sequential,
            boolean purge,
            boolean updateStatistics,
            boolean updateStatisticsAll,
            boolean dropStatistics,
            boolean dropStatisticsAll,
            String indexNameForStatistics) {
        super(tableId);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "instantiating AlterTableConstantOperation for table {%s.%s} with ColumnInfo {%s} and constraintActions {%s}",sd!=null?sd.getSchemaName():"default",tableName, Arrays.toString(columnInfo), Arrays.toString(constraintActions));
        this.sd                     = sd;
        this.tableName              = tableName;
        this.tableConglomerateId    = tableConglomerateId;
        this.columnInfo             = columnInfo;
        this.constraintActions      = (ConstraintConstantOperation[]) constraintActions;
        this.lockGranularity        = lockGranularity;
        this.compressTable          = compressTable;
        this.behavior               = behavior;
        this.sequential             = sequential;
        this.purge          		= purge;
        this.updateStatistics     	= updateStatistics;
        this.updateStatisticsAll    = updateStatisticsAll;
        this.dropStatistics     	= dropStatistics;
        this.dropStatisticsAll    = dropStatisticsAll;
        this.indexNameForStatistics = indexNameForStatistics;
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sd != null, "schema descriptor is null");
    }

    public	String	toString() {
        return "ALTER TABLE " + tableName;
    }

    /**
     * Run this constant action.
     *
     * @param activation the activation in which to run the action
     * @throws StandardException if an error happens during execution
     * of the action
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction with activation %s",activation);

        TransactionController userTxnController = activation.getTransactionController();
        SpliceTransactionManager child = (SpliceTransactionManager)userTxnController.startNestedUserTransaction(false,false);
        ((SpliceTransaction)child.getRawTransaction()).elevate(Long.toString(tableConglomerateId).getBytes());
        lcc = activation.getLanguageConnectionContext();
        tc = child;
        lcc.pushNestedTransaction(child);
        try {
            //body is executing inside of a child transaction now
            executeConstantActionBody(activation);
        } catch(StandardException se){
            child.abort();
            throw se;
        } finally {
            clearState();
        }
        child.commit();
    }

    /**
     * @see RowSource#getValidColumns
     */
    public FormatableBitSet getValidColumns() {
        SpliceLogUtils.trace(LOG, "getValidColumns");
        // All columns are valid
        return null;
    }

    /*private helper methods*/
    private void executeConstantActionBody(Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActionBody with activation %s",activation);

        // Save references to the main structures we need.
        this.activation = activation;
        lcc = activation.getLanguageConnectionContext();
        dd = lcc.getDataDictionary();
        dm = dd.getDependencyManager();
        tc = lcc.getTransactionExecute();
        //Following if is for inplace compress. Compress using temporary
        //tables to do the compression is done later in this method.
        if (compressTable) {
            if (purge) {
                td = dd.getTableDescriptor(tableId);
                if (td == null) {
                    throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
                }
                if (purge) // XXX TODO JLEACH Add Job Similar to Index Job to Purge SI Deleted / Aborted Records
                    purgeRows(tc);
                return;
            }
        }

        if (handleStatistics()) return;
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
        getTableDescriptor();

//        if (truncateTable)
//            dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);
//        else
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
//        if(truncateTable)
//            dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);
//        else
            dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

        int numRows = manageColumnInfo();
        // adjust dependencies on user defined types
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );

        executeConstraintActions(activation, numRows);
        adjustLockGranularity();

        // Are we doing a compress table?
        if (compressTable) {
            compressTable();
        }

        // Are we doing a truncate table?
//        if (truncateTable) {
//            truncateTable();
//        }
    }

    private void adjustLockGranularity() throws StandardException {
        // Are we changing the lock granularity?
        if (lockGranularity != '\0') {
            if (SanityManager.DEBUG) {
                if (lockGranularity != 'T' && lockGranularity != 'R') {
                    SanityManager.THROWASSERT("lockGranularity expected to be 'T'or 'R', not " + lockGranularity);
                }
            }
            // update the TableDescriptor
            td.setLockGranularity(lockGranularity);
            // update the DataDictionary
            dd.updateLockGranularity(td, sd, lockGranularity, tc);
        }
    }

    private void executeConstraintActions(Activation activation, int numRows) throws StandardException {
        if(constraintActions==null) return; //no constraints to apply, so nothing to do
        boolean tableScanned = numRows>=0;
        if(numRows<0)
            numRows = 0;


        for (int conIndex = 0; conIndex < constraintActions.length; conIndex++) {
            ConstraintConstantOperation cca = constraintActions[conIndex];
            if (cca instanceof CreateConstraintConstantOperation) {
                int constraintType = cca.getConstraintType();

					      /* Some constraint types require special checking:
					       *   Check		 - table must be empty, for now
					       *   Primary Key - table cannot already have a primary key
					       */
                switch (constraintType) {
                    case DataDictionary.PRIMARYKEY_CONSTRAINT:
                        // Check to see if a constraint of the same type
                        // already exists
                        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
                        if (cdl.getPrimaryKey() != null) {
                            throw StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_FAILED1, td.getQualifiedName());
                        }

                        if (!tableScanned) {
                            tableScanned = true;
                            numRows = getSemiRowCount(tc);
                        }
                        break;
                    case DataDictionary.CHECK_CONSTRAINT:
                        if (!tableScanned){
                            tableScanned = true;
                            numRows = getSemiRowCount(tc);
                        }
                        if (numRows > 0){
								            /*
								            ** We are assuming that there will only be one
								            ** check constraint that we are adding, so it
								            ** is ok to do the check now rather than try
								            ** to lump together several checks.
								            */
                            ConstraintConstantOperation.validateConstraint(
                                    cca.getConstraintName(), ((CreateConstraintConstantOperation)cca).getConstraintText(),
                                    td, lcc, true);
                        }
                        break;
                }
            } else {
                if (SanityManager.DEBUG) {
                    if (!(cca instanceof DropConstraintConstantOperation)) {
                        SanityManager.THROWASSERT("constraintActions[" + conIndex + "] expected to be instanceof " +
                                "DropConstraintConstantOperation not " +cca.getClass().getName());
                    }
                }
            }
            constraintActions[conIndex].executeConstantAction(activation);
        }
    }

    private int manageColumnInfo() throws StandardException{
        if(columnInfo==null){
            //we are not doing a column-based action, so just return
            return -1;
        }
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
            numRows = getSemiRowCount(tc);
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
                    addNewColumnToTable(ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE:
                case ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT:
                    modifyColumnDefault(ix);
                    break;
                case ColumnInfo.MODIFY_COLUMN_TYPE:
                    modifyColumnType(ix);
                case ColumnInfo.MODIFY_COLUMN_CONSTRAINT:
                    modifyColumnConstraint(columnInfo[ix].name,true);
                    break;
                case ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL:
                    if(!tableScanned){
                        tableScanned=true;
                        numRows = getSemiRowCount(tc);
                    }
                    // check that the data in the column is not null
                    String colNames[]  = new String[1];
                    colNames[0]        = columnInfo[ix].name;
                    boolean nullCols[] = new boolean[1];

									      /* note validateNotNullConstraint returns true if the
									      * column is nullable
									      */
                    if (validateNotNullConstraint(colNames, nullCols, numRows, lcc, SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN)) {
										        /* nullable column - modify it to be not null
										         * This is O.K. at this point since we would have
										         * thrown an exception if any data was null
										         */
                        modifyColumnConstraint(columnInfo[ix].name, false);
                    }
                    break;
                case ColumnInfo.DROP:
                    dropColumnFromTable(columnInfo[ix].name);
                default:
                    SanityManager.THROWASSERT("Unexpected action in AlterTableConstantAction");
            }
        }
        return numRows;
    }

    private void getTableDescriptor() throws StandardException {
        if (tableConglomerateId == 0) {
            td = dd.getTableDescriptor(tableId);
            if (td == null) {
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
            }
            tableConglomerateId = td.getHeapConglomerateId();
        }

        // XXX - TODO JL CANNOT LOCK lockTableForDDL(tc, tableConglomerateId, true);

        td = dd.getTableDescriptor(tableId);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }
    }

    private boolean handleStatistics() throws StandardException {
        if (updateStatistics) {
            updateStatistics();
            return true;
        }

        if (dropStatistics) {
            dropStatistics();
            return true;
        }
        return false;
    }

    /**
     * Clear the state of this constant action.
     */
    private void clearState() {
        SpliceLogUtils.trace(LOG, "clearState");
        // DERBY-3009: executeConstantAction() stores some of its state in
        // instance variables instead of local variables for convenience.
        // These variables should be cleared after the execution of the
        // constant action has completed, so that the objects they reference
        // can be garbage collected.
        td = null;
        lcc = null;
        dd = null;
        dm = null;
        tc = null;
        activation = null;
    }

    /**
     * Drop statistics of either all the indexes on the table or only one
     * specific index depending on what user has requested.
     *
     * @throws StandardException
     */
    private void dropStatistics() throws StandardException {
        SpliceLogUtils.trace(LOG, "dropStatistics");
        td = dd.getTableDescriptor(tableId);
        dd.startWriting(lcc);
        dm.invalidateFor(td, DependencyManager.UPDATE_STATISTICS, lcc);
        if (dropStatisticsAll) {
            dd.dropStatisticsDescriptors(td.getUUID(), null, tc);
        } else {
            ConglomerateDescriptor cd = dd.getConglomerateDescriptor(indexNameForStatistics, sd, false);
            dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
        }
    }

    /**
     * Update statistics of either all the indexes on the table or only one
     * specific index depending on what user has requested.
     *
     * @throws StandardException
     */
    private void updateStatistics() throws StandardException {
        SpliceLogUtils.trace(LOG, "updateStatistics");
        ConglomerateDescriptor[] cds;
        td = dd.getTableDescriptor(tableId);
        if (updateStatisticsAll) {
            cds = null;
        } else {
            cds = new ConglomerateDescriptor[1];
            cds[0] = dd.getConglomerateDescriptor(indexNameForStatistics, sd, false);
        }
        dd.getIndexStatsRefresher(false).runExplicitly(lcc, td, cds, "ALTER TABLE");
    }
    /**
     * Purge committed deleted rows from conglomerate.
     * <p>
     * Scans the table and purges any committed deleted rows from the 
     * table.  If all rows on a page are purged then page is also 
     * reclaimed.
     * <p>
     *
     * @param tc                transaction controller to use to do updates.
     *
     **/
    @SuppressWarnings("UnusedParameters")
    private void purgeRows(TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "purgeRows not implemented yet");
        throw StandardException.unexpectedUserException(new Exception("purgeRows not implemented yet"));
    }

    /**
     * Workhorse for adding a new column to a table.
     *
     * @param   ix 			the index of the column specfication in the ALTER
     *						statement-- currently we allow only one.
     * @exception StandardException 	thrown on failure.
     */
    @SuppressWarnings("unchecked")
    private void addNewColumnToTable(int ix) throws StandardException {
        SpliceLogUtils.trace(LOG, "addNewColumnToTable %d",ix);
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(columnInfo[ix].name);
        DataValueDescriptor storableDV;
        int colNumber = td.getMaxColumnID() + ix;

		/* We need to verify that the table does not have an existing
		 * column with the same name before we try to add the new
		 * one as addColumnDescriptor() is a void method.
		 */
        if (columnDescriptor != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,columnDescriptor.getDescriptorType(),
                    columnInfo[ix].name,td.getDescriptorType(),td.getQualifiedName());
        }

        if (columnInfo[ix].defaultValue != null)
            storableDV = columnInfo[ix].defaultValue;
        else
            storableDV = columnInfo[ix].dataType.getNull();

        // Add the column to the conglomerate.(Column ids in store are 0-based)
        ((SpliceTransaction)((SpliceTransactionManager)tc).getRawTransaction()).elevate(Bytes.toBytes(Long.toString(td.getHeapConglomerateId())));
        tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, storableDV,  columnInfo[ix].dataType.getCollationType());

        UUID defaultUUID = columnInfo[ix].newDefaultUUID;

		/* Generate a UUID for the default, if one exists
		 * and there is no default id yet.
		 */
        if (columnInfo[ix].defaultInfo != null && defaultUUID == null) {
            defaultUUID = dd.getUUIDFactory().createUUID();
        }

        // Add the column to syscolumns.
        // Column ids in system tables are 1-based
        columnDescriptor =
                new ColumnDescriptor(
                        columnInfo[ix].name,
                        colNumber + 1,
                        columnInfo[ix].dataType,
                        columnInfo[ix].defaultValue,
                        columnInfo[ix].defaultInfo,
                        td,
                        defaultUUID,
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc
                );

        dd.addDescriptor(columnDescriptor, td,DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        // now add the column to the tables column descriptor list.
        td.getColumnDescriptorList().add(columnDescriptor);

        if (columnDescriptor.isAutoincrement()) {
            updateNewAutoincrementColumn(columnInfo[ix].name, columnInfo[ix].autoincStart, columnInfo[ix].autoincInc);
        }

        // Update the new column to its default, if it has a non-null default
        if (columnDescriptor.hasNonNullDefault()) {
            updateNewColumnToDefault(columnDescriptor);
        }

        //
        // Add dependencies. These can arise if a generated column depends
        // on a user created function.
        //
        addColumnDependencies( lcc, dd, td, columnInfo[ix] );

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
    private void dropColumnFromTable(String columnName ) throws StandardException {
        SpliceLogUtils.trace(LOG, "dropColumnFromTable %s",columnName);

        boolean cascade = (behavior == StatementType.DROP_CASCADE);
        // drop any generated columns which reference this column
        ColumnDescriptorList generatedColumnList = td.getGeneratedColumns();
        int generatedColumnCount = generatedColumnList.size();
        ArrayList cascadedDroppedColumns = new ArrayList();
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
                        throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, dm.getActionString(DependencyManager.DROP_COLUMN),
                                columnName, "GENERATED COLUMN", generatedColumnName);
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
            throw StandardException.newException(
                    SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,dm.getActionString(DependencyManager.DROP_COLUMN),
                    "THE *LAST* COLUMN " + columnName,"TABLE",td.getQualifiedName() );
        }

        // now drop dependent generated columns
        for (Object cascadedDroppedColumn : cascadedDroppedColumns) {
            String generatedColumnName = (String) cascadedDroppedColumn;
            activation.addWarning(StandardException.newWarning(SQLState.LANG_GEN_COL_DROPPED, generatedColumnName, td.getName()));

            //
            // We can only recurse 2 levels since a generation clause cannot
            // refer to other generated columns.
            //
            dropColumnFromTable(generatedColumnName);
        }

        /*
         * Cascaded drops of dependent generated columns may require us to
         * rebuild the table descriptor.
         */
        td = dd.getTableDescriptor(tableId);

        ColumnDescriptor columnDescriptor = td.getColumnDescriptor( columnName );

        // We already verified this in bind, but do it again
        if (columnDescriptor == null) {
            throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, columnName, td.getQualifiedName());
        }

        int size = td.getColumnDescriptorList().size();
        droppedColumnPosition = columnDescriptor.getPosition();

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
                            throw StandardException.newException(
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
        ConstraintDescriptor[] toBeRemoved =
                new ConstraintDescriptor[csdl_size];

        // let's go downwards, don't want to get messed up while removing
        for (int i = csdl_size - 1; i >= 0; i--)
        {
            ConstraintDescriptor cd = csdl.elementAt(i);
            int[] referencedColumns = cd.getReferencedColumns();
            int numRefCols = referencedColumns.length, j;
            boolean changed = false;
            for (j = 0; j < numRefCols; j++)
            {
                if (referencedColumns[j] > droppedColumnPosition)
                    changed = true;
                if (referencedColumns[j] == droppedColumnPosition)
                    break;
            }
            if (j == numRefCols)			// column not referenced
            {
                if ((cd instanceof CheckConstraintDescriptor) && changed)
                {
                    dd.dropConstraintDescriptor(cd, tc);
                    for (j = 0; j < numRefCols; j++)
                    {
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
                throw StandardException.newException(
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
            dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT,
                    lcc);

            dropConstraint(cd, td, newCongloms, activation, lcc, true);
            activation.addWarning(
                    StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                            cd.getConstraintName(), td.getName()));
        }

        for (int i = tbr_size - 1; i >= 0; i--) {
            ConstraintDescriptor cd = toBeRemoved[i];
            dropConstraint(cd, td, newCongloms, activation, lcc, false);

            activation.addWarning(
                    StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                            cd.getConstraintName(), td.getName()));

            if (cascade)
            {
                ConstraintDescriptorList fkcdl = dd.getForeignKeys(cd.getUUID());
                for (int j = 0; j < fkcdl.size(); j++)
                {
                    ConstraintDescriptor fkcd = fkcdl.elementAt(j);
                    dm.invalidateFor(fkcd,
                            DependencyManager.DROP_CONSTRAINT,
                            lcc);

                    dropConstraint(fkcd, td,
                            newCongloms, activation, lcc, true);

                    activation.addWarning(
                            StandardException.newWarning(
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
        createNewBackingCongloms(newCongloms, null);

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

        spliceDropColumn();

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
        for (int i = columnDescriptor.getPosition(), j = 0; i < size; i++, j++)
        {
            ColumnDescriptor cd = tab_cdl.elementAt(i);
            dd.dropColumnDescriptor(td.getUUID(), cd.getColumnName(), tc);
            cd.setPosition(i);
            if (cd.isAutoincrement())
            {
                cd.setAutoinc_create_or_modify_Start_Increment(
                        ColumnDefinitionNode.CREATE_AUTOINCREMENT);
            }

            cdlArray[j] = cd;
        }
        dd.addDescriptorArray(cdlArray, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        // By this time, the column has been removed from the table descriptor.
        // Now, go through all the triggers and regenerate their trigger action
        // SPS and rebind the generated trigger action sql. If the trigger
        // action is using the dropped column, it will get detected here. If
        // not, then we will have generated the internal trigger action sql
        // which matches the trigger action sql provided by the user.
        //
        // eg of positive test case
        // create table atdc_16_tab1 (a1 integer, b1 integer, c1 integer);
        // create table atdc_16_tab2 (a2 integer, b2 integer, c2 integer);
        // create trigger atdc_16_trigger_1
        //    after update of b1 on atdc_16_tab1
        //    REFERENCING NEW AS newt
        //    for each row
        //    update atdc_16_tab2 set c2 = newt.c1
        // The internal representation for the trigger action before the column
        // is dropped is as follows
        // 	 update atdc_16_tab2 set c2 =
        //   org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
        //   getONewRow().getInt(3)
        // After the drop column shown as below
        //   alter table DERBY4998_SOFT_UPGRADE_RESTRICT drop column c11
        // The above internal representation of tigger action sql is not
        // correct anymore because column position of c1 in atdc_16_tab1 has
        // now changed from 3 to 2. Following while loop will regenerate it and
        // change it to as follows
        // 	 update atdc_16_tab2 set c2 =
        //   org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
        //   getONewRow().getInt(2)
        //
        // We could not do this before the actual column drop, because the
        // rebind would have still found the column being dropped in the
        // table descriptor and hence use of such a column in the trigger
        // action rebind would not have been caught.

        //For the table on which ALTER TABLE is getting performed, find out
        // all the SPSDescriptors that use that table as a provider. We are
        // looking for SPSDescriptors that have been created internally for
        // trigger action SPSes. Through those SPSDescriptors, we will be
        // able to get to the triggers dependent on the table being altered
        //Following will get all the dependent objects that are using
        // ALTER TABLE table as provider
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
                        columnDroppedAndTriggerDependencies(trdToBeDropped,
                                cascade, columnName);
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

    // For the trigger, get the trigger action sql provided by the user
    // in the create trigger sql. This sql is saved in the system
    // table. Since a column has been dropped from the trigger table,
    // the trigger action sql may not be valid anymore. To establish
    // that, we need to regenerate the internal representation of that
    // sql and bind it again.
    private void columnDroppedAndTriggerDependencies(TriggerDescriptor trd,
                                                     boolean cascade, String columnName)
            throws StandardException {
        dd.dropTriggerDescriptor(trd, tc);

        // Here we get the trigger action sql and use the parser to build
        // the parse tree for it.
        SchemaDescriptor compSchema;
        compSchema = dd.getSchemaDescriptor(trd.getSchemaDescriptor().getUUID(), null);
        CompilerContext newCC = lcc.pushCompilerContext(compSchema);
        Parser	pa = newCC.getParser();
        StatementNode stmtnode = (StatementNode)pa.parseStatement(trd.getTriggerDefinition());
        lcc.popCompilerContext(newCC);
        // Do not delete following. We use this in finally clause to
        // determine if the CompilerContext needs to be popped.
        newCC = null;

        try {
            // We are interested in ColumnReference classes in the parse tree
            CollectNodesVisitor visitor = new CollectNodesVisitor(ColumnReference.class);
            stmtnode.accept(visitor);

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
            compSchema = dd.getSchemaDescriptor(trd.getSchemaDescriptor().getUUID(), null);
            newCC = lcc.pushCompilerContext(compSchema);
            newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
            pa = newCC.getParser();
            stmtnode = (StatementNode)pa.parseStatement(triggerActionSPSD.getText());
            // need a current dependent for bind
            newCC.setCurrentDependent(triggerActionSPSD.getPreparedStatement());
            stmtnode.bindStatement();
        } catch (StandardException se)
        {
            //Need to catch for few different kinds of sql states depending
            // on what kind of trigger action sql is using the column being
            // dropped. Following are examples for different sql states
            //
            //SQLState.LANG_COLUMN_NOT_FOUND is thrown for following usage in
            // trigger action sql of column being dropped atdc_12.b
            //        create trigger atdc_12_trigger_1 after update
            //           of a on atdc_12
            //           for each row
            //           select a,b from atdc_12
            //
            //SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE is thrown for following
            // usage in trigger action sql of column being dropped
            // atdc_14_tab2a2 with restrict clause
            //        create trigger atdc_14_trigger_1 after update
            //           on atdc_14_tab1 REFERENCING NEW AS newt
            //           for each row
            //           update atdc_14_tab2 set a2 = newt.a1
            //
            // SQLState.LANG_DB2_INVALID_COLS_SPECIFIED is thrown for following
            //  usage in trigger action sql of column being dropped
            //  ATDC_13_TAB1_BACKUP.c11 with restrict clause
            //         create trigger ATDC_13_TAB1_trigger_1 after update
            //           on ATDC_13_TAB1 for each row
            //           INSERT INTO ATDC_13_TAB1_BACKUP
            //           SELECT C31, C32 from ATDC_13_TAB3
            //
            //SQLState.LANG_TABLE_NOT_FOUND is thrown for following scenario
            //   create view ATDC_13_VIEW2 as select c12 from ATDC_13_TAB3 where c12>0
            //Has following trigger defined
            //         create trigger ATDC_13_TAB1_trigger_3 after update
            //           on ATDC_13_TAB1 for each row
            //           SELECT * from ATDC_13_VIEW2
            // Ane drop column ATDC_13_TAB3.c12 is issued
            if (se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND)||
                    (se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE) ||
                            (se.getMessageId().equals(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED) ||
                                    (se.getMessageId().equals(SQLState.LANG_TABLE_NOT_FOUND)))))
            {
                if (cascade)
                {
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
                            dm.getActionString(DependencyManager.DROP_COLUMN),
                            columnName, "TRIGGER",
                            trd.getName() );
                }
            } else
                throw se;
        } finally {
            if (newCC != null)
                lcc.popCompilerContext(newCC);
        }

        // If we are here, then it means that the column being dropped
        // is not getting used in the trigger action.
        //
        // We have recreated the trigger action SPS and recollected the
        // column positions for trigger columns and trigger action columns
        // getting accessed through REFERENCING clause because
        // drop column can affect the column positioning of existing
        // columns in the table. We will save that in the system table.
        dd.addDescriptor(trd, sd,
                DataDictionary.SYSTRIGGERS_CATALOG_NUM,
                false, tc);
    }

    private void modifyColumnType(int ix) throws StandardException {
        SpliceLogUtils.trace(LOG, "modifyColumnType %d",ix);
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
        dd.addDescriptor(newColumnDescriptor, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
    }

    /**
     * Workhorse for modifying column level constraints.
     * Right now it is restricted to modifying a null constraint to a not null
     * constraint.
     */
    private void modifyColumnConstraint(String colName, boolean nullability) throws StandardException {
        SpliceLogUtils.trace(LOG, "modifyColumnConstraint %s with nullability %s",colName, nullability);
        ColumnDescriptor columnDescriptor = td.getColumnDescriptor(colName), newColumnDescriptor;

        // Get the type and change the nullability
        DataTypeDescriptor dataType = columnDescriptor.getType().getNullabilityType(nullability);

        //check if there are any unique constraints to update
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
        int columnPostion = columnDescriptor.getPosition();
        for (int i = 0; i < cdl.size(); i++)
        {
            ConstraintDescriptor cd = cdl.elementAt(i);
            if (cd.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT)
            {
                ColumnDescriptorList columns = cd.getColumnDescriptors();
                for (int count = 0; count < columns.size(); count++)
                {
                    if (columns.elementAt(count).getPosition() != columnPostion)
                        break;

                    //get backing index
                    ConglomerateDescriptor desc =
                            td.getConglomerateDescriptor(cd.getConglomerateId());

                    //check if the backing index was created when the column
                    //not null ie is backed by unique index
                    if (!desc.getIndexDescriptor().isUnique())
                        break;

                    // replace backing index with a unique when not null index.
                    recreateUniqueConstraintBackingIndexAsUniqueWhenNotNull(
                            desc, td, activation, lcc);
                }
            }
        }

        newColumnDescriptor =
                new ColumnDescriptor(colName,
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
        dd.addDescriptor(newColumnDescriptor, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
    }
    /**
     * Workhorse for modifying the default value of a column.
     *
     * @param       ix 		the index of the column specfication in the ALTER
     *						statement-- currently we allow only one.
     * @exception	StandardException, thrown on error.
     */
    private void modifyColumnDefault(int ix) throws StandardException {
        SpliceLogUtils.trace(LOG, "modifyColumnDefault %d",ix);
        ColumnDescriptor columnDescriptor =
                td.getColumnDescriptor(columnInfo[ix].name);
        int columnPosition = columnDescriptor.getPosition();

        // Clean up after the old default, if non-null
        if (columnDescriptor.hasNonNullDefault()) {
            // Invalidate off of the old default
            DefaultDescriptor defaultDescriptor = new DefaultDescriptor(dd, columnInfo[ix].oldDefaultUUID,
                    td.getUUID(), columnPosition);


            dm.invalidateFor(defaultDescriptor, DependencyManager.MODIFY_COLUMN_DEFAULT, lcc);

            // Drop any dependencies
            dm.clearDependencies(lcc, defaultDescriptor);
        }

        UUID defaultUUID = columnInfo[ix].newDefaultUUID;

		/* Generate a UUID for the default, if one exists
		 * and there is no default id yet.
		 */
        if (columnInfo[ix].defaultInfo != null && defaultUUID == null) {
            defaultUUID = dd.getUUIDFactory().createUUID();
        }

		/* Get a ColumnDescriptor reflecting the new default */
        columnDescriptor = new ColumnDescriptor(
                columnInfo[ix].name,
                columnPosition,
                columnInfo[ix].dataType,
                columnInfo[ix].defaultValue,
                columnInfo[ix].defaultInfo,
                td,
                defaultUUID,
                columnInfo[ix].autoincStart,
                columnInfo[ix].autoincInc,
                columnInfo[ix].autoinc_create_or_modify_Start_Increment
        );

        // Update the ColumnDescriptor with new default info
        dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
        dd.addDescriptor(columnDescriptor, td,
                DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

        if (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT) {
            // adding an autoincrement default-- calculate the maximum value
            // of the autoincrement column.
            long maxValue = getColumnMax(td, columnInfo[ix].name,
                    columnInfo[ix].autoincInc);
            dd.setAutoincrementValue(tc, td.getUUID(), columnInfo[ix].name,
                    maxValue, true);
        } else if (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART) {
            dd.setAutoincrementValue(tc, td.getUUID(), columnInfo[ix].name,columnInfo[ix].autoincStart, false);
        }
        // else we are simply changing the default value
    }

    /**
     * routine to process compress table or ALTER TABLE <t> DROP COLUMN <c>;
     * <p>
     * Uses class level variable "compressTable" to determine if processing
     * compress table or drop column:
     *     if (!compressTable)
     *         must be drop column.
     * <p>
     * Handles rebuilding of base conglomerate and all necessary indexes.
     **/
    private void compressTable() throws StandardException {

    }
    private void spliceDropColumn() throws StandardException {
        SpliceLogUtils.trace(LOG, "compressTable");
        long					newHeapConglom;
        Properties				properties = new Properties();
        RowLocation				rl;

        if (SanityManager.DEBUG) {
            if (lockGranularity != '\0') {
                SanityManager.THROWASSERT(
                        "lockGranularity expected to be '\0', not " +
                                lockGranularity);
            }
            SanityManager.ASSERT(! compressTable || columnInfo == null,
                    "columnInfo expected to be null");
            SanityManager.ASSERT(constraintActions == null,
                    "constraintActions expected to be null");
        }

        ExecRow emptyHeapRow  = td.getEmptyExecRow();
        int[]   collation_ids = td.getColumnCollationIds();

        compressHeapCC =
                tc.openConglomerate(
                        td.getHeapConglomerateId(),
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);

        rl = compressHeapCC.newRowLocationTemplate();

        // Get the properties on the old heap
        compressHeapCC.getInternalTablePropertySet(properties);
        compressHeapCC.close();
        compressHeapCC = null;

        // Create an array to put base row template
        baseRow = new ExecRow[bulkFetchSize];
        baseRowArray = new DataValueDescriptor[bulkFetchSize][];
        validRow = new boolean[bulkFetchSize];

		/* Set up index info */
        getAffectedIndexes(td);

        // Get an array of RowLocation template
        compressRL = new RowLocation[bulkFetchSize];
        indexRows  = new ExecIndexRow[numIndexes];
        if (!compressTable)
        {
            // must be a drop column, thus the number of columns in the
            // new template row and the collation template is one less.
            ExecRow newRow =
                    activation.getExecutionFactory().getValueRow(
                            emptyHeapRow.nColumns() - 1);

            int[]   new_collation_ids = new int[collation_ids.length - 1];

            for (int i = 0; i < newRow.nColumns(); i++)
            {
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
        }
        setUpAllSorts(emptyHeapRow, rl);

        // Start by opening a full scan on the base table.
        openBulkFetchScan(td.getHeapConglomerateId());

        // Get the estimated row count for the sorters
        estimatedRowCount = compressHeapGSC.getEstimatedRowCount();

        // Create the array of base row template
        for (int i = 0; i < bulkFetchSize; i++)
        {
            // create a base row template
            baseRow[i] = td.getEmptyExecRow();
            baseRowArray[i] = baseRow[i].getRowArray();
            compressRL[i] = compressHeapGSC.newRowLocationTemplate();
        }

        // calculate column order for new table
        TxnView parentTxn = ((SpliceTransactionManager)lcc.getTransactionExecute()).getActiveStateTxn();
        TxnView txn = ((SpliceTransactionManager)tc).getActiveStateTxn();
//        String txnId = TransactionUtils.getTransactionId(tc);
        int[] co = DataDictionaryUtils.getColumnOrdering(parentTxn, tableId);
        int[] newco = DataDictionaryUtils.getColumnOrderingAfterDropColumn(co, droppedColumnPosition);
        IndexColumnOrder[] columnOrdering = null;
        if (newco != null && newco.length > 0) {
            columnOrdering = new IndexColumnOrder[newco.length];
            for (int i = 0; i < newco.length; ++i) {
                columnOrdering[i] = new IndexColumnOrder(newco[i]);
            }
        }
        // Create a new table -- use createConglomerate() to avoid confusing calls to createAndLoad()
        newHeapConglom = tc.createConglomerate("heap", emptyHeapRow.getRowArray(),
                columnOrdering, collation_ids,
                properties, TransactionController.IS_DEFAULT);


        // Start a tentative txn to notify DDL('alter table drop column') change
        Txn tentativeTransaction;
        try {
            tentativeTransaction = TransactionLifecycle.getLifecycleManager().beginTransaction();
        } catch (IOException e) {
            LOG.error("Couldn't start transaction for tentative DDL operation");
            throw Exceptions.parseException(e);
        }
        final long tableConglomId = td.getHeapConglomerateId();

        ColumnInfo[] allColumnInfo = DataDictionaryUtils.getColumnInfo(td);
        TentativeDropColumnDesc tentativeDropColumnDesc =
                new TentativeDropColumnDesc(
                        tableId,
                        newHeapConglom,
                        td.getHeapConglomerateId(),
                        allColumnInfo,
                        droppedColumnPosition);

        DDLChange ddlChange = new DDLChange(tentativeTransaction,
                DDLChangeType.DROP_COLUMN);
        ddlChange.setTentativeDDLDesc(tentativeDropColumnDesc);
        ddlChange.setParentTxn(((SpliceTransactionManager)tc).getActiveStateTxn());

        notifyMetadataChange(ddlChange);

        HTableInterface table = SpliceAccessManager.getHTable(Long.toString(tableConglomId).getBytes());
        try {
            //Add a handler to drop a column to all regions
            tentativeDropColumn(table, newHeapConglom, tableConglomId, ddlChange);

            //wait for all past txns to complete
            Txn dropColumnTransaction = getDropColumnTransaction(parentTxn, txn,
                    tentativeTransaction,  tableConglomId);

            // Copy data from old table to the new table
            //copyToConglomerate(newHeapConglom, tentativeTransaction.getTransactionIdString(), co);
            copyToConglomerate(newHeapConglom, parentTxn);
            dropColumnTransaction.commit();
        }
        catch (IOException e) {
            tc.abort();
            throw Exceptions.parseException(e);
        }
        finally{
            Closeables.closeQuietly(table);
        }

        closeBulkFetchScan();

        // Set the "estimated" row count
        ScanController compressHeapSC = tc.openScan(
                newHeapConglom,
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                null,
                null,
                0,
                null,
                null,
                0);

        compressHeapSC.setEstimatedRowCount(rowCount);

        compressHeapSC.close();

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

        // Update all indexes
        if (compressIRGs.length > 0)
        {
            updateAllIndexes(newHeapConglom, dd,td,tc);
        }

		/* Update the DataDictionary
		 * RESOLVE - this will change in 1.4 because we will get
		 * back the same conglomerate number
		 */
        // Get the ConglomerateDescriptor for the heap
        long oldHeapConglom       = td.getHeapConglomerateId();
        ConglomerateDescriptor cd =
                td.getConglomerateDescriptor(oldHeapConglom);

        // Update sys.sysconglomerates with new conglomerate #
        dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dm.invalidateFor(td, DependencyManager.COMPRESS_TABLE, lcc);

        // Drop the old conglomerate
        tc.dropConglomerate(oldHeapConglom);

        cleanUp();
    }

    private Txn getDropColumnTransaction(TxnView parentTransaction,
                                         TxnView wrapperTransaction,
                                         Txn tentativeTransaction,
                                         long tableConglomId) throws StandardException {
        Txn dropColumnTransaction;
        try {
            dropColumnTransaction = TransactionLifecycle.getLifecycleManager().chainTransaction(wrapperTransaction, Txn.IsolationLevel.SNAPSHOT_ISOLATION, false, Bytes.toBytes(Long.toString(tableConglomId)),tentativeTransaction);
        } catch (IOException e) {
            LOG.error("Couldn't commit transaction for tentative DDL operation");
            // TODO must cleanup tentative DDL change
            throw Exceptions.parseException(e);
        }

        // Wait for past transactions to die
        List<TxnView> toIgnore = Arrays.asList(parentTransaction, wrapperTransaction, dropColumnTransaction);
        long activeTxnId;
        try {
            activeTxnId = waitForConcurrentTransactions(dropColumnTransaction, toIgnore,tableConglomId);
        } catch (IOException e) {
            LOG.error("Unexpected error while waiting for past transactions to complete", e);
            throw Exceptions.parseException(e);
        }
        if (activeTxnId>=0) {
            throw StandardException.newException(SQLState.LANG_SERIALIZABLE,
                    new RuntimeException(String.format("Transaction %d is still active", activeTxnId)));
        }
        return dropColumnTransaction;
    }

    private void tentativeDropColumn(HTableInterface table,
                                     long newConglomId,
                                     long oldConglomId,
                                     DDLChange ddlChange) throws StandardException{
        JobFuture future = null;
        JobInfo info = null;
        TentativeDropColumnDesc desc = (TentativeDropColumnDesc)ddlChange.getTentativeDDLDesc();
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
            LOG.error("Couldn't drop column on existing regions", e);
            try {
                table.close();
            } catch (IOException e1) {
                LOG.warn("Couldn't close table", e1);
            }
            throw Exceptions.parseException(e);
        }finally {
            cleanupFuture(future);
        }

    }

    private void cleanupFuture(JobFuture future) throws StandardException {
        if (future!=null) {
            try {
                future.cleanup();
            } catch (ExecutionException e) {
                LOG.error("Couldn't cleanup future", e);
                //noinspection ThrowFromFinallyBlock
                throw Exceptions.parseException(e.getCause());
            }
        }
    }

    private void copyToConglomerate(long toConglomId, TxnView dropColumnTxn) throws StandardException {

        String user = lcc.getSessionUserId();
        Snowflake snowflake = SpliceDriver.driver().getUUIDGenerator();
        long sId = snowflake.nextUUID();
        if (activation.isTraced()) {
            activation.getLanguageConnectionContext().setXplainStatementId(sId);
        }
        StatementInfo statementInfo =
                new StatementInfo(String.format("alter table %s.%s drop %s", td.getSchemaName(), td.getName(), columnInfo[0].name),
                        user,dropColumnTxn, 1, SpliceDriver.driver().getUUIDGenerator());
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
                    statementInfo.getStatementUuid(), opInfo.getOperationUuid(), activation.isTraced());
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

    private void updateAllIndexes(long newHeapConglom, DataDictionary dd,TableDescriptor td, TransactionController tc) throws StandardException {
        /*
         * Update all of the indexes on a table when doing a bulk insert
         * on an empty table.
         */
        SpliceLogUtils.trace(LOG, "updateAllIndexes on new heap conglom %d",newHeapConglom);
        long[] newIndexCongloms = new long[numIndexes];
        for (int index = 0; index < numIndexes; index++) {
            updateIndex(newHeapConglom, dd, tc, td,index, newIndexCongloms);
        }
    }

    protected void updateIndex(long newHeapConglom, DataDictionary dd,
                               TransactionController tc, TableDescriptor td, int index, long[] newIndexCongloms)
            throws StandardException {
        SpliceLogUtils.trace(LOG, "updateIndex on new heap conglom %d for index %d with newIndexCongloms %s",
                newHeapConglom, index, Arrays.toString(newIndexCongloms));

        TransactionController parent = lcc.getTransactionExecute();

        // Get the ConglomerateDescriptor for the index
        ConglomerateDescriptor cd = td.getConglomerateDescriptor(indexConglomerateNumbers[index]);

        // Build the properties list for the new conglomerate
        ConglomerateController indexCC =
                tc.openConglomerate(
                        indexConglomerateNumbers[index],
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE);
        Properties properties = getIndexProperties(newHeapConglom, index, cd, indexCC);


        // We can finally drain the sorter and rebuild the index
        // Populate the index.

        boolean statisticsExist  = false;

        DataValueDescriptor[] rowArray = indexRows[index].getRowArray();
        ColumnOrdering[] columnOrder = ordering[index];
        int[] collationIds = collation[index];
        doIndexUpdate(dd,td,tc, index, newIndexCongloms, cd, properties, statisticsExist,rowArray,columnOrder,collationIds);
        try{
            // Populate indexes
            IndexDescriptor indexDescriptor = cd.getIndexDescriptor();
            int[] baseColumnPositions = indexDescriptor.baseColumnPositions();
            boolean unique = indexDescriptor.isUnique();
            boolean uniqueWithDuplicateNulls = indexDescriptor.isUniqueWithDuplicateNulls();
            boolean[] isAscending = indexDescriptor.isAscending();
            boolean[] descColumns = BooleanArrays.not(isAscending);

            byte[] writeTable = Long.toString(newHeapConglom).getBytes();
            TxnView parentTxn = ((SpliceTransactionManager)parent).getActiveStateTxn();
            Txn tentativeTransaction;
            try {
                tentativeTransaction = TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,writeTable);
            } catch (IOException e) {
                LOG.error("Couldn't start transaction for tentative DDL operation");
                throw Exceptions.parseException(e);
            }
            TentativeIndexDesc tentativeIndexDesc = new TentativeIndexDesc(newIndexCongloms[index], newHeapConglom,
                    baseColumnPositions, unique,
                    uniqueWithDuplicateNulls,
                    SpliceUtils.bitSetFromBooleanArray(descColumns));
            DDLChange ddlChange = new DDLChange(tentativeTransaction,
                    DDLChangeType.CREATE_INDEX);
            ddlChange.setTentativeDDLDesc(tentativeIndexDesc);
            ddlChange.setParentTxn(((SpliceTransactionManager)tc).getActiveStateTxn());

            notifyMetadataChange(ddlChange);

            HTableInterface table = SpliceAccessManager.getHTable(writeTable);
            try{
                // Add the indexes to the exisiting regions
                createIndex(activation, ddlChange, table, td);

                Txn indexTransaction = getIndexTransaction(parent, tc, tentativeTransaction, newHeapConglom);

                populateIndex(activation, baseColumnPositions, descColumns,
                        newHeapConglom, table,tc,
                        indexTransaction, tentativeTransaction.getCommitTimestamp(),tentativeIndexDesc);
                //only commit the index transaction if the job actually completed
                tentativeTransaction.commit();
            }finally{
                Closeables.closeQuietly(table);
            }
        } catch (StandardException se) {
            tc.abort();
            throw se;
        } catch (Throwable t) {
            tc.abort();
            throw Exceptions.parseException(t);
        }

		/* Update the DataDictionary
		 *
		 * Update sys.sysconglomerates with new conglomerate #, we need to
		 * update all (if any) duplicate index entries sharing this same
		 * conglomerate.
		 */
        dd.updateConglomerateDescriptor(
                td.getConglomerateDescriptors(indexConglomerateNumbers[index]),
                newIndexCongloms[index],
                tc);

        // Drop the old conglomerate
        tc.dropConglomerate(indexConglomerateNumbers[index]);
        tc.commit();
    }

    protected void doIndexUpdate(DataDictionary dd,
                                TableDescriptor td,
                                TransactionController tc,
                                 int index, long[] newIndexCongloms,
                                 ConglomerateDescriptor cd, Properties properties,
                                 boolean statisticsExist,
                                 DataValueDescriptor[] rowArray,
                                 ColumnOrdering[] columnOrder,
                                 int[] collationIds) throws StandardException {
        RowLocationRetRowSource cCount;
        sorters[index].completedInserts();
        sorters[index] = null;

        if (td.statisticsExist(cd)) {
            cCount = new CardinalityCounter( tc.openSortRowSource(sortIds[index]));

            statisticsExist = true;
        } else {
            cCount = new CardinalityCounter( tc.openSortRowSource(sortIds[index]));
        }

        newIndexCongloms[index] =
                tc.createAndLoadConglomerate(
                        "BTREE",
                        rowArray,
                        columnOrder,
                        collationIds,
                        properties,
                        TransactionController.IS_DEFAULT,
                        cCount,
                        null);

        //For an index, if the statistics already exist, then drop them.
        //The statistics might not exist for an index if the index was
        //created when the table was empty.
        //
        //For all alter table actions, including ALTER TABLE COMPRESS,
        //for both kinds of indexes (ie. one with preexisting statistics
        //and with no statistics), create statistics for them if the table
        //is not empty.
        if (statisticsExist)
            dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);

        long numRows;
        if ((numRows = ((CardinalityCounter)cCount).getRowCount()) > 0) {
            long[] c = ((CardinalityCounter)cCount).getCardinality();
            for (int i = 0; i < c.length; i++) {
                StatisticsDescriptor statDesc =
                        new StatisticsDescriptor(
                                dd,
                                dd.getUUIDFactory().createUUID(),
                                cd.getUUID(),
                                td.getUUID(),
                                "I",
                                new StatisticsImpl(numRows, c[i]),
                                i + 1);

                dd.addDescriptor(
                        statDesc,
                        null,   // no parent descriptor
                        DataDictionary.SYSSTATISTICS_CATALOG_NUM,
                        true,   // no error on duplicate.
                        tc);
            }
        }
    }

    private Properties getIndexProperties(long newHeapConglom, int index, ConglomerateDescriptor cd, ConglomerateController indexCC) throws StandardException {
        // Get the properties on the old index
        Properties properties = new Properties();
        indexCC.getInternalTablePropertySet(properties);

		    /* Create the properties that language supplies when creating the
		     * the index.  (The store doesn't preserve these.)
		     */
        int indexRowLength = indexRows[index].nColumns();
        properties.put("baseConglomerateId", Long.toString(newHeapConglom));
        if (cd.getIndexDescriptor().isUnique()) {
            properties.put( "nUniqueColumns", Integer.toString(indexRowLength - 1));
        } else {
            properties.put( "nUniqueColumns", Integer.toString(indexRowLength));
        }
        if(cd.getIndexDescriptor().isUniqueWithDuplicateNulls()) {
            properties.put( "uniqueWithDuplicateNulls", Boolean.toString(true));
        }
        properties.put( "rowLocationColumn", Integer.toString(indexRowLength - 1));
        properties.put( "nKeyFields", Integer.toString(indexRowLength));

        indexCC.close();
        return properties;
    }


    /**
     * Get info on the indexes on the table being compressed.
     *
     * @exception StandardException		Thrown on error
     */
    protected int getAffectedIndexes(TableDescriptor td) throws StandardException {
        SpliceLogUtils.trace(LOG, "getAffectedIndexes");

        IndexLister	indexLister = td.getIndexLister( );

		/* We have to get non-distinct index row generaters and conglom numbers
		 * here and then compress it to distinct later because drop column
		 * will need to change the index descriptor directly on each index
		 * entry in SYSCONGLOMERATES, on duplicate indexes too.
		 */
        compressIRGs = indexLister.getIndexRowGenerators();
        numIndexes = compressIRGs.length;
        indexConglomerateNumbers = indexLister.getIndexConglomerateNumbers();

        if (! (compressTable))		// then it's drop column
        {
            ArrayList newCongloms = new ArrayList();
            for (int i = 0; i < compressIRGs.length; i++)
            {
                int[] baseColumnPositions = compressIRGs[i].baseColumnPositions();
                int j;
                for (j = 0; j < baseColumnPositions.length; j++)
                    if (baseColumnPositions[j] == droppedColumnPosition) break;
                if (j == baseColumnPositions.length)	// not related
                    continue;

                if (baseColumnPositions.length == 1 ||
                        (behavior == StatementType.DROP_CASCADE && compressIRGs[i].isUnique()))
                {
                    numIndexes--;
					/* get first conglomerate with this conglom number each time
					 * and each duplicate one will be eventually all dropped
					 */
                    ConglomerateDescriptor cd = td.getConglomerateDescriptor
                            (indexConglomerateNumbers[i]);

                    dropConglomerate(cd, td, true, newCongloms, activation,
                            activation.getLanguageConnectionContext());

                    compressIRGs[i] = null;		// mark it
                    continue;
                }
                // give an error for unique index on multiple columns including
                // the column we are to drop (restrict), such index is not for
                // a constraint, because constraints have already been handled
                if (compressIRGs[i].isUnique())
                {
                    ConglomerateDescriptor cd = td.getConglomerateDescriptor
                            (indexConglomerateNumbers[i]);
                    throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                            dm.getActionString(DependencyManager.DROP_COLUMN),
                            columnInfo[0].name, "UNIQUE INDEX",
                            cd.getConglomerateName() );
                }
            }

			/* If there are new backing conglomerates which must be
			 * created to replace a dropped shared conglomerate
			 * (where the shared conglomerate was dropped as part
			 * of a "drop conglomerate" call above), then create
			 * them now.  We do this *after* dropping all dependent
			 * conglomerates because we don't want to waste time
			 * creating a new conglomerate if it's just going to be
			 * dropped again as part of another "drop conglomerate"
			 * call.
			 */
            createNewBackingCongloms(newCongloms, indexConglomerateNumbers);

            IndexRowGenerator[] newIRGs = new IndexRowGenerator[numIndexes];
            long[] newIndexConglomNumbers = new long[numIndexes];

            for (int i = 0, j = 0; i < numIndexes; i++, j++)
            {
                while (compressIRGs[j] == null)
                    j++;

                int[] baseColumnPositions = compressIRGs[j].baseColumnPositions();
                newIRGs[i] = compressIRGs[j];
                newIndexConglomNumbers[i] = indexConglomerateNumbers[j];

                boolean[] isAscending = compressIRGs[j].isAscending();
                boolean reMakeArrays = false;
                int size = baseColumnPositions.length;
                for (int k = 0; k < size; k++)
                {
                    if (baseColumnPositions[k] > droppedColumnPosition)
                        baseColumnPositions[k]--;
                    else if (baseColumnPositions[k] == droppedColumnPosition)
                    {
                        baseColumnPositions[k] = 0;		// mark it
                        reMakeArrays = true;
                    }
                }
                if (reMakeArrays)
                {
                    size--;
                    int[] newBCP = new int[size];
                    boolean[] newIsAscending = new boolean[size];
                    for (int k = 0, step = 0; k < size; k++)
                    {
                        if (step == 0 && baseColumnPositions[k + step] == 0)
                            step++;
                        newBCP[k] = baseColumnPositions[k + step];
                        newIsAscending[k] = isAscending[k + step];
                    }
                    IndexDescriptor id = compressIRGs[j].getIndexDescriptor();
                    id.setBaseColumnPositions(newBCP);
                    id.setIsAscending(newIsAscending);
                    id.setNumberOfOrderedColumns(id.numberOfOrderedColumns() - 1);
                }
            }
            compressIRGs = newIRGs;
            indexConglomerateNumbers = newIndexConglomNumbers;
        }

		/* Now we are done with updating each index descriptor entry directly
		 * in SYSCONGLOMERATES (for duplicate index as well), from now on, our
		 * work should apply ONLY once for each real conglomerate, so we
		 * compress any duplicate indexes now.
		 */
        Object[] compressIndexResult =
                compressIndexArrays(indexConglomerateNumbers, compressIRGs);

        if (compressIndexResult != null)
        {
            indexConglomerateNumbers = (long[]) compressIndexResult[1];
            compressIRGs = (IndexRowGenerator[]) compressIndexResult[2];
            numIndexes = indexConglomerateNumbers.length;
        }

        indexedCols = new FormatableBitSet(getIndexedColumnSize(td));
        for (int index = 0; index < numIndexes; index++) {
            int[] colIds = compressIRGs[index].getIndexDescriptor().baseColumnPositions();

            for (int colId : colIds) {
                indexedCols.set(colId);
            }
        }
        return numIndexes;
    }

    protected int getIndexedColumnSize(TableDescriptor td) {
        return compressTable ? td.getNumberOfColumns() + 1 : td.getNumberOfColumns();
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
    private void createNewBackingCongloms(ArrayList newConglomActions, long [] ixCongNums) throws StandardException {
        SpliceLogUtils.trace(LOG, "createNewBackingCongloms");
        for (Object newConglomAction : newConglomActions) {
            CreateIndexConstantOperation ca =
                    (CreateIndexConstantOperation) newConglomAction;

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
            ConglomerateDescriptor[] tdCDs =
                    td.getConglomerateDescriptors(oldCongNum);

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
     * Set up to update all of the indexes on a table when doing a bulk insert
     * on an empty table.
     *
     * @exception StandardException					thrown on error
     */
    private void setUpAllSorts(ExecRow sourceRow, RowLocation rl) throws StandardException {
        SpliceLogUtils.trace(LOG, "setUpAllSorts");
        ordering        = new ColumnOrdering[numIndexes][];
        collation       = new int[numIndexes][];
        needToDropSort  = new boolean[numIndexes];
        sortIds         = new long[numIndexes];

		/* For each index, build a single index row and a sorter. */
        for (int index = 0; index < numIndexes; index++)
        {
            // create a single index row template for each index
            indexRows[index] = compressIRGs[index].getIndexRowTemplate();


            // Get an index row based on the base row
            // (This call is only necessary here because we need to pass a
            // template to the sorter.)
            compressIRGs[index].getIndexRow(
                    sourceRow, rl, indexRows[index], null);

            // Setup collation id array to be passed in on call to create index.
            collation[index] =
                    compressIRGs[index].getColumnCollationIds(
                            td.getColumnDescriptorList());

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * No need to try to enforce uniqueness here as
			 * index should be valid.
			 */
            int[]       baseColumnPositions =
                    compressIRGs[index].baseColumnPositions();
            boolean[]   isAscending         =
                    compressIRGs[index].isAscending();
            int         numColumnOrderings  =
                    baseColumnPositions.length + 1;

			/* We can only reuse the wrappers when doing an
			 * external sort if there is only 1 index.  Otherwise,
			 * we could get in a situation where 1 sort reuses a
			 * wrapper that is still in use in another sort.
			 */
            boolean reuseWrappers = (numIndexes == 1);

            SortObserver    sortObserver =
                    new BasicSortObserver(
                            false, false, indexRows[index], reuseWrappers);

            ordering[index] = new ColumnOrdering[numColumnOrderings];
            for (int ii =0; ii < numColumnOrderings - 1; ii++)
            {
                ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
            }
            ordering[index][numColumnOrderings - 1] =
                    new IndexColumnOrder(numColumnOrderings - 1);

            // create the sorters
            sortIds[index] =
                    tc.createSort(
                            null,
                            indexRows[index].getRowArrayClone(),
                            ordering[index],
                            sortObserver,
                            false,			        // not in order
                            estimatedRowCount,		// est rows
                            -1				        // est row size, -1 means no idea
                    );
        }

        sorters = new SortController[numIndexes];

        // Open the sorts
        for (int index = 0; index < numIndexes; index++)
        {
            sorters[index] = tc.openSort(sortIds[index]);
            needToDropSort[index] = true;
        }
    }

    // RowSource interface


    /**
     * @see RowSource#getNextRowFromRowSource
     * @exception StandardException on error
     */
    public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException {
        SpliceLogUtils.trace(LOG, "getNextRowFromRowSource");
        currentRow = null;
        // Time for a new bulk fetch?
        if ((! doneScan) &&
                (currentCompressRow == bulkFetchSize || !validRow[currentCompressRow]))
        {
            int bulkFetched;

            bulkFetched = compressHeapGSC.fetchNextGroup(baseRowArray, compressRL);

            doneScan = (bulkFetched != bulkFetchSize);
            currentCompressRow = 0;
            rowCount += bulkFetched;
            for (int index = 0; index < bulkFetched; index++)
            {
                validRow[index] = true;
            }
            for (int index = bulkFetched; index < bulkFetchSize; index++)
            {
                validRow[index] = false;
            }
        }

        if (validRow[currentCompressRow])
        {
            if (compressTable)
            {
                currentRow = baseRow[currentCompressRow];
            }
            else
            {
                if (currentRow == null)
                {
                    currentRow =
                            activation.getExecutionFactory().getValueRow(
                                    baseRowArray[currentCompressRow].length - 1);
                }

                for (int i = 0; i < currentRow.nColumns(); i++)
                {
                    currentRow.setColumn(
                            i + 1,
                            i < droppedColumnPosition - 1 ?
                                    baseRow[currentCompressRow].getColumn(i+1) :
                                    baseRow[currentCompressRow].getColumn(i+1+1));
                }
            }
            currentCompressRow++;
        }

        if (currentRow != null)
        {
			/* Let the target preprocess the row.  For now, this
			 * means doing an in place clone on any indexed columns
			 * to optimize cloning and so that we don't try to drain
			 * a stream multiple times.
			 */
            if (compressIRGs.length > 0)
            {
				/* Do in-place cloning of all of the key columns */
                currentRow =  currentRow.getClone(indexedCols);
            }

            return currentRow.getRowArray();
        }

        return null;
    }

    /**
     * @see RowSource#needsToClone
     */
    public boolean needsToClone() {
        SpliceLogUtils.trace(LOG, "needsToClone");
        return(true);
    }

    /**
     * @see RowSource#closeRowSource
     */
    public void closeRowSource() {
        SpliceLogUtils.trace(LOG, "closeRowSource");
        // Do nothing here - actual work will be done in close()
    }


    // RowLocationRetRowSource interface

    /**
     * @see RowLocationRetRowSource#needsRowLocation
     */
    public boolean needsRowLocation() {
        SpliceLogUtils.trace(LOG, "needsRowLocation");
        // Only true if table has indexes
        return (numIndexes > 0);
    }

    /**
     * @see RowLocationRetRowSource#rowLocation
     * @exception StandardException on error
     */
    public void rowLocation(RowLocation rl)
            throws StandardException
    {
        SpliceLogUtils.trace(LOG, "rowLocation");
		/* Set up sorters, etc. if 1st row and there are indexes */
        if (compressIRGs.length > 0)
        {
            objectifyStreamingColumns();

			/* Put the row into the indexes.  If sequential, 
			 * then we only populate the 1st sorter when compressing
			 * the heap.
			 */
            int maxIndex = compressIRGs.length;
            if (maxIndex > 1 && sequential)
            {
                maxIndex = 1;
            }
            for (int index = 0; index < maxIndex; index++)
            {
                insertIntoSorter(index, rl);
            }
        }
    }

    private void objectifyStreamingColumns() throws StandardException {
        // Objectify any the streaming columns that are indexed.
        for (int i = 0; i < currentRow.getRowArray().length; i++) {
			/* Object array is 0-based,
			 * indexedCols is 1-based.
			 */
            if (! indexedCols.get(i + 1)) {
                continue;
            }

            if (currentRow.getRowArray()[i] instanceof StreamStorable) {
                currentRow.getRowArray()[i].getObject();
            }
        }
    }

    private void insertIntoSorter(int index, RowLocation rl) throws StandardException {
        // Get a new object Array for the index
        indexRows[index].getNewObjectArray();
        // Associate the index row with the source row
        compressIRGs[index].getIndexRow(currentRow,
                (RowLocation) rl.cloneValue(false),
                indexRows[index],
                null);

        // Insert the index row into the matching sorter
        sorters[index].insert(indexRows[index].getRowArray());
    }

    protected void	cleanUp() throws StandardException {
        if (compressHeapCC != null) {
            compressHeapCC.close();
            compressHeapCC = null;
        }

        if (compressHeapGSC != null) {
            closeBulkFetchScan();
        }

        // Close each sorter
        if (sorters != null) {
            for (int index = 0; index < compressIRGs.length; index++) {
                if (sorters[index] != null) {
                    sorters[index].completedInserts();
                }
                sorters[index] = null;
            }
        }

        if (needToDropSort != null) {
            for (int index = 0; index < needToDropSort.length; index++) {
                if (needToDropSort[index]) {
                    tc.dropSort(sortIds[index]);
                    needToDropSort[index] = false;
                }
            }
        }
    }

    protected TableDescriptor getTableDescriptor(DataDictionary dd) throws StandardException {
        TableDescriptor td;
        td = dd.getTableDescriptor(tableId);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }
        if (tableConglomerateId == 0) {
            tableConglomerateId = td.getHeapConglomerateId();
        }
        return td;
    }

    // class implementation

    /**
     * Return the "semi" row count of a table.  We are only interested in
     * whether the table has 0, 1 or > 1 rows.
     *
     *
     * @return Number of rows (0, 1 or > 1) in table.
     *
     * @exception StandardException		Thrown on failure
     */
    private int getSemiRowCount(TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "getSemiRowCount");
        int			   numRows = 0;

        ScanController sc = tc.openScan(td.getHeapConglomerateId(),
                false,	// hold
                0,	    // open read only
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                RowUtil.EMPTY_ROW_BITSET, // scanColumnList
                null,	// start position
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
    private void updateNewColumnToDefault(ColumnDescriptor columnDescriptor) throws StandardException {
        SpliceLogUtils.trace(LOG, "updateNewColumnToDefault with columnDescriptor %s",columnDescriptor);
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

    private static void executeUpdate(LanguageConnectionContext lcc, String updateStmt) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeUpdate with statement {%s}",updateStmt);
        PreparedStatement ps = lcc.prepareInternalStatement(updateStmt);

        // This is a substatement; for now, we do not set any timeout
        // for it. We might change this behaviour later, by linking
        // timeout to its parent statement's timeout settings.
        ResultSet rs = ps.executeSubStatement(lcc, true, 0L);
        rs.close();
    }

    /**
     * computes the minimum/maximum value in a column of a table.
     */
    private long getColumnMax(TableDescriptor td, String columnName, long increment) throws StandardException {
        SpliceLogUtils.trace(LOG, "getColumnMax for column {%s}.{%s} with increment %d",td.getName(), columnName, increment);
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

    private void openBulkFetchScan(long heapConglomNumber) throws StandardException {
        SpliceLogUtils.trace(LOG, "openBulkFetchScan on heap %d",heapConglomNumber);
        doneScan = false;
        compressHeapGSC = tc.openGroupFetchScan(
                heapConglomNumber,
                false,	// hold
                0,	// open base table read only
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE,
                null,    // all fields as objects
                null,	// startKeyValue
                0,		// not used when giving null start posn.
                null,	// qualifier
                null,	// stopKeyValue
                0);		// not used when giving null stop posn.
    }

    private void closeBulkFetchScan() throws StandardException {
        compressHeapGSC.close();
        compressHeapGSC = null;
    }

    /**
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
    private void updateNewAutoincrementColumn(String columnName, long initial, long increment) throws StandardException {
        SpliceLogUtils.trace(LOG, "updateNewAutoincrementColumn %s with initial %d and increment %d",columnName, initial, increment);
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
    /**
     * Make sure that the columns are non null
     * If any column is nullable, check that the data is null.
     *
     * @param	columnNames	names of columns to be checked
     * @param	nullCols	true if corresponding column is nullable
     * @param	numRows		number of rows in the table
     * @param	lcc		language context
     * @param	errorMsg	error message to use for exception
     *
     * @return true if any nullable columns found (nullable columns must have
     *		all non null data or exception is thrown
     * @exception StandardException on error
     */
    private boolean validateNotNullConstraint (
            String							columnNames[],
            boolean							nullCols[],
            int								numRows,
            LanguageConnectionContext		lcc,
            String							errorMsg ) throws StandardException {
        SpliceLogUtils.trace(LOG, "validateNotNullConstraint");
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
                throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
                        columnNames[colCtr],
                        td.getName());
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
        if (foundNullable && numRows > 0)
        {
            if (!ConstraintConstantOperation.validateConstraint(
                    null,
                    constraintText.toString(),
                    td,
                    lcc,
                    false)) {
                if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT)) {	//alter table add primary key
                    //soft upgrade mode
                    throw StandardException.newException(
                            SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT,
                            td.getQualifiedName());
                } else if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY)) {	//alter table add primary key
                    throw StandardException.newException(
                            SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY,
                            td.getQualifiedName());
                } else {	//alter table modify column not null
                    throw StandardException.newException(
                            SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN,
                            td.getQualifiedName(), columnNames[0]);
                }
            }
        }
        return foundNullable;
    }

    /**
     * Get rid of duplicates from a set of index conglomerate numbers and
     * index descriptors.
     *
     * @param	indexCIDS	array of index conglomerate numbers
     * @param	irgs		array of index row generaters
     *
     * @return value:		If no duplicates, returns NULL; otherwise,
     *						a size-3 array of objects, first element is an
     *						array of duplicates' indexes in the input arrays;
     *						second element is the compact indexCIDs; third
     *						element is the compact irgs.
     */
    private Object[] compressIndexArrays( long[] indexCIDS, IndexRowGenerator[] irgs) {
        SpliceLogUtils.trace(LOG, "compressIndexArrays");
		/* An efficient way to compress indexes.  From one end of workSpace,
		 * we save unique conglom IDs; and from the other end we save
		 * duplicate indexes' indexes.  We save unique conglom IDs so that
		 * we can do less amount of comparisons.  This is efficient in
		 * space as well.  No need to use hash table.
		 */
        long[] workSpace = new long[indexCIDS.length];
        int j = 0, k = indexCIDS.length - 1;
        for (int i = 0; i < indexCIDS.length; i++) {
            int m;
            for (m = 0; m < j; m++){		// look up our unique set
                if (indexCIDS[i] == workSpace[m]){	// it's a duplicate
                    workSpace[k--] = i;		// save dup index's index
                    break;
                }
            }
            if (m == j)
                workSpace[j++] = indexCIDS[i];	// save unique conglom id
        }

        if(j>=indexCIDS.length) return null; //no duplicates

        long[] newIndexCIDS = new long[j];
        IndexRowGenerator[] newIrgs = new IndexRowGenerator[j];
        int[] duplicateIndexes = new int[indexCIDS.length - j];
        k = 0;
        // do everything in one loop
        for (int m = 0, n = indexCIDS.length - 1; m < indexCIDS.length; m++) {
            // we already gathered our indexCIDS and duplicateIndexes
            if (m < j)
                newIndexCIDS[m] = workSpace[m];
            else
                duplicateIndexes[indexCIDS.length - m - 1] = (int) workSpace[m];

            // stack up our irgs, indexSCOCIs, indexDCOCIs
            if ((n >= j) && (m == (int) workSpace[n]))
                n--;
            else {
                newIrgs[k] = irgs[m];
                k++;
            }
        }

        // construct return value
        Object[] returnValue = new Object[3]; // [indexSCOCIs == null ? 3 : 5];
        returnValue[0] = duplicateIndexes;
        returnValue[1] = newIndexCIDS;
        returnValue[2] = newIrgs;
        return returnValue;
    }
}
