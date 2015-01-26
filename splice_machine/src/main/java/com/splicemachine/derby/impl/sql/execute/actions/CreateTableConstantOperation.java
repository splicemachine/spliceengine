package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.derby.impl.sql.execute.RowUtil;
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
    public CreateTableConstantOperation(
            String			schemaName,
            String			tableName,
            int				tableType,
            ColumnInfo[]	columnInfo,
            ConstantAction[] constraintActions,
            Properties		properties,
            char			lockGranularity,
            boolean			onCommitDeleteRows,
            boolean			onRollbackDeleteRows) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.columnInfo = columnInfo;
        this.constraintActions = (ConstraintConstantOperation[]) constraintActions;
        this.properties = properties;
        this.lockGranularity = lockGranularity;
        this.onCommitDeleteRows = onCommitDeleteRows;
        this.onRollbackDeleteRows = onRollbackDeleteRows;

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

    public	String	toString() {
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
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");

		    /* Mark the activation as being for create table */
        activation.setForCreateTable();

        createTable(activation);
    }

    protected void createTable(Activation activation) throws StandardException {
        /*
         * Put the create table into a child transaction.
         * At this end of this action, no matter what, we will
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
         */
        dd.startWriting(lcc);

		    /* create the conglomerate to hold the table's rows
		     * RESOLVE - If we ever have a conglomerate creator
		     * that lets us specify the conglomerate number then
		     * we will need to handle it here.
		     */
        long conglomId = tc.createConglomerate(
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
            td = ddg.newTableDescriptor(tableName, sd, tableType, lockGranularity);
        } else {
            td = ddg.newTableDescriptor(tableName, sd, tableType, onCommitDeleteRows, onRollbackDeleteRows);
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
                        index++,
                        columnInfo[ix].dataType,
                        columnInfo[ix].defaultValue,
                        columnInfo[ix].defaultInfo,
                        td,
                        defaultUUID,
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc,
                        columnInfo[ix].autoinc_create_or_modify_Start_Increment
                );
            else
                columnDescriptor = new ColumnDescriptor(
                        columnInfo[ix].name,
                        index++,
                        columnInfo[ix].dataType,
                        columnInfo[ix].defaultValue,
                        columnInfo[ix].defaultInfo,
                        td,
                        defaultUUID,
                        columnInfo[ix].autoincStart,
                        columnInfo[ix].autoincInc
                );

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
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );

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
    }

    protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td,
                                                                    long conglomId, SchemaDescriptor sd,
                                                                    DataDescriptorGenerator ddg) throws StandardException{
        return ddg.newConglomerateDescriptor(conglomId, null, false, null, false, null, td.getUUID(), sd.getUUID());
    }


}

