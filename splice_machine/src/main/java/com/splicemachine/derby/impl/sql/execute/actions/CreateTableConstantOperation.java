/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.db.impl.sql.execute.RowUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class CreateTableConstantOperation extends DDLConstantOperation {
    private static final Logger LOG = Logger.getLogger(CreateTableConstantOperation.class);

    private char					lockGranularity;
    private boolean					onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
    private boolean					onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
    private String					tableName;
    private String					schemaName;
    private int						tableType;
    private ColumnInfo[]			columnInfo;
    // Contains CreateConstraintConstantOperation elements, but array ref is ConstraintConstantOperation[]
    protected ConstraintConstantOperation[]	constraintActions;
    private Properties				properties;
    private String delimited;
    private String escaped;
    private String lines;
    private String storedAs;
    private String location;



    /**
     *	Make the ConstantAction for a CREATE TABLE statement.
     *
     *  @param schemaName	name for the schema that table lives in.
     *  @param tableName	Name of table.
     *  @param tableType	Type of table (e.g., BASE, global temporary table).
     *  @param columnInfo	Information on all the columns in the table.
     *		 (REMIND tableDescriptor ignored)
     *  @param constraintActions	CreateConstraintConstantAction[] for constraints
     *  @param properties	Optional table properties
     * @param lockGranularity	The lock granularity.
     * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
     * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public CreateTableConstantOperation(
            String			schemaName,
            String			tableName,
            int				tableType,
            ColumnInfo[]	columnInfo,
            ConstantAction[] constraintActions,
            Properties		properties,
            char			lockGranularity,
            boolean			onCommitDeleteRows,
            boolean			onRollbackDeleteRows,
            String delimited,
            String escaped,
            String lines,
            String storedAs,
            String location
            ) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.columnInfo = columnInfo;
        this.constraintActions = (ConstraintConstantOperation[]) constraintActions;
        this.properties = properties;
        this.lockGranularity = lockGranularity;
        this.onCommitDeleteRows = onCommitDeleteRows;
        this.onRollbackDeleteRows = onRollbackDeleteRows;
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;

        if (SanityManager.DEBUG) {
            if (tableType == TableDescriptor.BASE_TABLE_TYPE && lockGranularity != TableDescriptor.TABLE_LOCK_GRANULARITY &&
                    lockGranularity != TableDescriptor.ROW_LOCK_GRANULARITY) {
                SanityManager.THROWASSERT("Unexpected value for lockGranularity = " + lockGranularity);
            }
            if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE && !onRollbackDeleteRows) {
                SanityManager.THROWASSERT("Unexpected value for onRollbackDeleteRows = false");
            }
            SanityManager.ASSERT(schemaName != null, "SchemaName is null");
        }
    }

    @Override
    public String toString() {
        if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
            return constructToString("DECLARE GLOBAL TEMPORARY TABLE ", tableName);
        else
            return constructToString("CREATE TABLE ", tableName);
    }


    /**
     *	This is the guts of the Execution-time logic for CREATE TABLE.
     *
     *	@see ConstantAction#executeConstantAction
     *
     * @exception StandardException		Thrown on failure
     */
    @Override
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");

		    /* Mark the activation as being for create table */
        activation.setForCreateTable();

        createTable(activation);
    }

    protected void createTable(Activation activation) throws StandardException {
        /*
         * Put the create table into a child transaction.
         * At this end of this action, nxo matter what, we will
         * either commit or roll back the transaction.
         *
         * This may not be strictly necessary (e.g. we might
         * be able to get away with not using child transactions
         * in this case since the data dictionary handles things,
         * but this way is certainly safer).
         */
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        /*
         * Note on transactional behavior:
         *
         * We want to ensure that CreateTable occurs within an internal transaction, so
         * that we can either commit or rollback the entire operation (in the event of failure).
         * In some past versions of Splice, we did this by explicitly constructing a child
         * transaction here, and then manually committing or rolling it back. However,
         * DB-1706 implements transaction savepoints, which allows us to transparently act
         * as if we are a direct user transaction, which in reality we are inside of a savepoint
         * operation, so we are still safe.
         *
         * Therefore, it doesn't look like we are doing much in terms of transaction management
         * here, but in fact we are fully transactional and within a savepoint context.
         */
        TransactionController tc = lcc.getTransactionExecute();

        ExecRow template;TableDescriptor td;ColumnDescriptor columnDescriptor;// setup for create conglomerate call:
        //   o create row template to tell the store what type of rows this
        //     table holds.
        //   o create array of collation id's to tell collation id of each
        //     column in table.
        template = RowUtil.getEmptyValueRow(columnInfo.length, lcc);


        int[] collation_ids = new int[columnInfo.length];

        List<String> pkColumnNames = null;
        ColumnOrdering[] columnOrdering = null;
        if (constraintActions != null) {
            for(ConstraintConstantOperation constantAction:constraintActions){
                if(constantAction.getConstraintType()== DataDictionary.PRIMARYKEY_CONSTRAINT){
                    pkColumnNames = Arrays.asList(((CreateConstraintConstantOperation) constantAction).columnNames);
                    columnOrdering = new IndexColumnOrder[pkColumnNames.size()];
                }else if(constantAction.getConstraintType()==DataDictionary.FOREIGNKEY_CONSTRAINT){
                    constantAction.validateSupported();
                }
            }
        }

        for (int ix = 0; ix < columnInfo.length; ix++) {
            ColumnInfo col_info = columnInfo[ix];
            if (pkColumnNames != null && pkColumnNames.contains(col_info.name)) {
                columnOrdering[pkColumnNames.indexOf(col_info.name)] = new IndexColumnOrder(ix);
            }
            // Get a template value for each column
            if (col_info.defaultValue != null) {
            /* If there is a default value, use it, otherwise use null */
                template.setColumn(ix + 1, col_info.defaultValue);
            }
            else {
                template.setColumn(ix + 1, col_info.dataType.getNull());
            }
            // get collation info for each column.
            collation_ids[ix] = col_info.dataType.getCollationType();
        }

        /*
         * Inform the data dictionary that we are about to write to it.
         * There are several calls to data dictionary "get" methods here
         * that might be done in "read" mode in the data dictionary, but
         * it seemed safer to do this whole operation in "write" mode.
         *
         * We tell the data dictionary we're done writing at the end of
         * the transaction.
         *
         * -sf- We don't need to set the ddl write mode in the lcc, because
         * we don't need to do two phase commit for create table statements.
         */
        dd.startWriting(lcc);

        // Put displayable table name into properties, which will ultimately be persisted
        // in the HTableDescriptor for convenient fetching where DataDictionary not available.
        if(properties==null)
            properties = new Properties();
        properties.setProperty(SIConstants.TABLE_DISPLAY_NAME_ATTR, this.tableName);

        /* create the conglomerate to hold the table's rows
		 * RESOLVE - If we ever have a conglomerate creator
		 * that lets us specify the conglomerate number then
		 * we will need to handle it here.
		 */
        long conglomId = tc.createConglomerate(storedAs!=null,
                "heap", // we're requesting a heap conglomerate
                template.getRowArray(), // row template
                columnOrdering, //column sort order - not required for heap
                collation_ids,
                properties, // properties
                tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE ?
                        (TransactionController.IS_TEMPORARY | TransactionController.IS_KEPT) :
                        TransactionController.IS_DEFAULT);

        SchemaDescriptor sd = DDLConstantOperation.getSchemaDescriptorForCreate(dd, activation, schemaName);

        //
        // Create a new table descriptor.
        //
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE ) {
            td = ddg.newTableDescriptor(tableName, sd, tableType, lockGranularity,columnInfo.length,
                    delimited,
                    escaped,
                    lines,
                    storedAs,
                    location
                    );
        } else {
            td = ddg.newTableDescriptor(tableName, sd, tableType, onCommitDeleteRows, onRollbackDeleteRows,columnInfo.length);
            td.setUUID(dd.getUUIDFactory().createUUID());
        }

        dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);

        // Save the TableDescriptor off in the Activation
        activation.setDDLTableDescriptor(td);

				/*
				 * NOTE: We must write the columns out to the system
				 * tables before any of the conglomerates, including
				 * the heap, since we read the columns before the
				 * conglomerates when building a TableDescriptor.
				 * This will hopefully reduce the probability of
				 * a deadlock involving those system tables.
				 */

        // for each column, stuff system.column
        int index = 1;

        ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnInfo.length];
        for (int ix = 0; ix < columnInfo.length; ix++) {
            UUID defaultUUID = columnInfo[ix].newDefaultUUID;
						/*
						 * Generate a UUID for the default, if one exists
						 * and there is no default id yet.
						 */
            if (columnInfo[ix].defaultInfo != null && defaultUUID == null) {
                defaultUUID = dd.getUUIDFactory().createUUID();
            }
            if (columnInfo[ix].autoincInc != 0)//dealing with autoinc column
                columnDescriptor = new ColumnDescriptor(
                        columnInfo[ix].name,
                        index,
                        index,
                        columnInfo[ix].dataType,
                        columnInfo[ix].defaultValue,
                        columnInfo[ix].defaultInfo,
                        td,
                        defaultUUID,
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc,
                        columnInfo[ix].autoinc_create_or_modify_Start_Increment,
                        columnInfo[ix].partitionPosition
                );
            else {
                columnDescriptor = new ColumnDescriptor(
                        columnInfo[ix].name,
                        index,
                        index,
                        columnInfo[ix].dataType,
                        columnInfo[ix].defaultValue,
                        columnInfo[ix].defaultInfo,
                        td,
                        defaultUUID,
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc,
                        columnInfo[ix].partitionPosition

                );
            }
            index++;
            /*
             * By default, we enable statistics collection on all keyed columns
             */
            if(pkColumnNames!=null && pkColumnNames.contains(columnDescriptor.getColumnName())){
               columnDescriptor.setCollectStatistics(true);
            }

            cdlArray[ix] = columnDescriptor;
        }

        dd.addDescriptorArray(cdlArray, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM,
                false, tc);

        // now add the column descriptors to the table.
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        Collections.addAll(cdl, cdlArray);

        /*
         * Create a conglomerate desciptor with the conglomId filled in and
         * add it.
         *
         * RESOLVE: Get information from the conglomerate descriptor which
         *          was provided.
         */
        ConglomerateDescriptor cgd = getTableConglomerateDescriptor(td, conglomId, sd, ddg);
        dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);

        // add the newly added conglomerate to the table descriptor
        ConglomerateDescriptorList conglomList = td.getConglomerateDescriptorList();
        conglomList.add(cgd);

		    /* Create any constraints */
        if (constraintActions != null) {
            /*
             * Do everything but FK constraints first,
             * then FK constraints on 2nd pass.
             */
            for (ConstraintConstantOperation constraintAction : constraintActions) {
                // skip fks
                if (!((CreateConstraintConstantOperation) constraintAction).isForeignKeyConstraint()) {
                    constraintAction.executeConstantAction(activation);
                }
            }

            for (ConstraintConstantOperation constraintAction : constraintActions) {
                // only foreign keys
                if (((CreateConstraintConstantOperation) constraintAction).isForeignKeyConstraint()) {
                    constraintAction.executeConstantAction(activation);
                }
            }
        }

        /*
         * Add dependencies. These can arise if a generated column depends
         * on a user created function.
         */
        for (ColumnInfo aColumnInfo : columnInfo) {
            addColumnDependencies(lcc, dd, td, aColumnInfo);
        }

        // The table itself can depend on the user defined types of its columns.
        adjustUDTDependencies(activation, columnInfo, false );

        if ( tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE ) {
            lcc.addDeclaredGlobalTempTable(td);
        }

        /*
         * Indicate that the CREATE TABLE statement itself depends on the
         * table it is creating. Normally such statement dependencies are
         * added during compilation, but here we have a bootstrapping issue
         * because the table doesn't exist until the CREATE TABLE statement
         * has been executed, so we had to defer the creation of this
         * dependency until now. (DERBY-4479)
         */
        dd.getDependencyManager().addDependency(activation.getPreparedStatement(), td, lcc.getContextManager());

        /*
         * Notify the DDL mechanism that a table is being created.
         *
         * In previous versions of Splice we would not use this mechanism: after all, the other servers
         *  don't really need to know that a table has been created, right?
         *
         *  The problem is that we rely on this mechanism to tell us when our data dictionary caches are to be
         *  invalidated; If we don't invalidate the caches on commit or rollback, we can get bleed-over
         *  where the table appears to exist even though it was rolled back (see CreateTableTransactionIT for some
         *  Integration tests around this).
         *
         *  To keep things simple we piggy-back on the existing DDL mechanism to ensure that our caches
         *  get cleared on commit/rollback. If this proves too expensive, then a custom local-memory-only trigger
         *  mechanism can be written for this and CREATE_SCHEMA
         */
        long txnId = ((SpliceTransactionManager)tc).getRawTransaction().getActiveStateTxn().getTxnId();
        DDLMessage.DDLChange change =DDLMessage.DDLChange.newBuilder().setDdlChangeType(DDLMessage.DDLChangeType.CREATE_TABLE).setTxnId(txnId).build();
        tc.prepareDataDictionaryChange(DDLDriver.driver().ddlController().notifyMetadataChange(change));
    }

    protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td,
                                                                    long conglomId, SchemaDescriptor sd,
                                                                    DataDescriptorGenerator ddg) throws StandardException{
        return ddg.newConglomerateDescriptor(conglomId, null, false, null, false, null, td.getUUID(), sd.getUUID());
    }

    public String getScopeName() {
        return String.format("Create Table %s", tableName);
    }

}

