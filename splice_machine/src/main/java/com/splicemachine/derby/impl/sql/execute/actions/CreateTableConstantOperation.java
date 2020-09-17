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

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLDriver;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.procedures.external.DistributedCreateExternalTableJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.procedures.external.DistributedGetSchemaExternalJob;
import com.splicemachine.procedures.external.GetSchemaExternalResult;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.system.CsvOptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.sql.SQLWarning;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private boolean isExternal;
    private Properties				properties;
    private String delimited;
    private String escaped;
    private String lines;
    private String storedAs;
    private String location;
    private String compression;
    private boolean mergeSchema;
    private boolean presplit;
    private boolean isLogicalKey;
    private String splitKeyPath;
    private String columnDelimiter;
    private String characterDelimiter;
    private String timestampFormat;
    private String dateFormat;
    private String timeFormat;


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
            String timeFormat) {
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
        this.isExternal = isExternal;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.compression = compression;
        this.mergeSchema = mergeSchema;
        this.presplit = presplit;
        this.isLogicalKey = isLogicalKey;
        this.splitKeyPath = splitKeyPath;
        this.columnDelimiter = columnDelimiter;
        this.characterDelimiter = characterDelimiter;
        this.timestampFormat = timestampFormat;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;

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

        // setup for create conglomerate call:
        //   o create row template to tell the store what type of rows this
        //     table holds.
        ExecRow template = createRowTemplate(lcc);

        //   o create array of collation id's to tell collation id of each
        //     column in table.
        int[] collation_ids = createCollationIds();

        //
        // create pkColumnNames and columnOrdering
        //
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
        properties.setProperty(SIConstants.SCHEMA_DISPLAY_NAME_ATTR, this.schemaName);
        properties.setProperty(SIConstants.TABLE_DISPLAY_NAME_ATTR, this.tableName);

        byte[][] splitKeys = null;
        try {
            if (presplit) {
                splitKeys = calculateSplitKeys(activation, columnOrdering, columnInfo);
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
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
                        TransactionController.IS_DEFAULT,
                splitKeys);
        SchemaDescriptor sd = DDLConstantOperation.getSchemaDescriptorForCreate(dd, activation, schemaName);

        try {
            if (tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) {
                if (dd.databaseReplicationEnabled() || dd.schemaReplicationEnabled(schemaName)) {
                    PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
                    admin.enableTableReplication(Long.toString(conglomId));
                }
            }
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
        //
        // Create a new table descriptor.
        //
        TableDescriptor td;
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
        td = createTableDescriptor(activation, lcc, dd, tc, sd, ddg);

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
        ColumnDescriptor[] cdlArray = createColumnDescriptorArray(dd, td, pkColumnNames, index);

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
        dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc, false);

        // add the newly added conglomerate to the table descriptor
        ConglomerateDescriptorList conglomList = td.getConglomerateDescriptorList();
        conglomList.add(cgd);

        /* Create any constraints */
        createConstraints(activation);

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

        // is this an external file ?
        if(storedAs != null) {
            externalTablesCreate(activation, txnId);
        }
    }

    private int[] createCollationIds() {
        int[] collation_ids = new int[columnInfo.length];
        for (int ix = 0; ix < columnInfo.length; ix++) {
            ColumnInfo col_info = columnInfo[ix];
            // get collation info for each column.
            collation_ids[ix] = col_info.dataType.getCollationType();
        }
        return collation_ids;
    }

    private ExecRow createRowTemplate(LanguageConnectionContext lcc) throws StandardException {
        ExecRow template = RowUtil.getEmptyValueRow(columnInfo.length, lcc);

        for (int ix = 0; ix < columnInfo.length; ix++) {
            ColumnInfo col_info = columnInfo[ix];
            // Get a template value for each column
            if (col_info.defaultValue != null) {
                /* If there is a default value, use it, otherwise use null */
                template.setColumn(ix + 1, col_info.defaultValue);
            } else {
                template.setColumn(ix + 1, col_info.dataType.getNull());
            }
        }
        return template;
    }

    private TableDescriptor createTableDescriptor(Activation activation, LanguageConnectionContext lcc,
                                                  DataDictionary dd, TransactionController tc, SchemaDescriptor sd,
                                                  DataDescriptorGenerator ddg) throws StandardException {
        TableDescriptor td;
        if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE ) {
            td = ddg.newTableDescriptor(tableName, sd, tableType, lockGranularity,columnInfo.length,
                    delimited,
                    escaped,
                    lines,
                    storedAs,
                    location,
                    compression,
                    false,
                    false,
                    null
            );
        } else {
            td = ddg.newTableDescriptor(tableName, sd, tableType, onCommitDeleteRows, onRollbackDeleteRows,columnInfo.length);
            td.setUUID(dd.getUUIDFactory().createUUID());
        }


        String createAsVersion2String = null;
        createAsVersion2String = PropertyUtil.getCachedDatabaseProperty(lcc, Property.CREATE_TABLES_AS_VERSION_2);
        boolean createAsVersion2 = createAsVersion2String != null && Boolean.valueOf(createAsVersion2String);

        dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc, createAsVersion2);

        // Save the TableDescriptor off in the Activation
        activation.setDDLTableDescriptor(td);
        return td;
    }

    /**
     * get the partitions, and send a command to create an external empty file
     */
    @SuppressFBWarnings("REC_CATCH_EXCEPTION") // SpotBugs false positive
    private void externalTablesCreate(Activation activation, long txnId) throws StandardException {

        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        int[] partitionby = activation.getDDLTableDescriptor().getPartitionBy();
        String jobGroup = userId + " <" + txnId + ">";
        try {

            // test constraint only if the external file exits
            DistributedFileSystem fileSystem = SIDriver.driver().getFileSystem(location);
            FileInfo fileInfo = fileSystem.getInfo(location);
            if (!fileInfo.exists() || fileInfo.isEmptyDirectory()) {
                // need to create the external file if the location provided is empty
                String pathToParent = location.substring(0, location.lastIndexOf("/"));
                ImportUtils.validateWritable(pathToParent, false);
                EngineDriver.driver().getOlapClient().execute(new DistributedCreateExternalTableJob(delimited,
                        escaped, lines, storedAs, location, compression, partitionby, jobGroup, columnInfo));

            } else if (!fileInfo.isDirectory()) {
                throw StandardException.newException(SQLState.DIRECTORY_REQUIRED, location);
            } else {
                CsvOptions csvOptions = new CsvOptions(delimited, escaped, lines);
                Future<GetSchemaExternalResult> futureResult = EngineDriver.driver().getOlapClient().
                        submit(new DistributedGetSchemaExternalJob(location, jobGroup, storedAs, mergeSchema, csvOptions));
                GetSchemaExternalResult result = null;
                SConfiguration config = EngineDriver.driver().getConfiguration();

                while (result == null) {
                    try {
                        result = futureResult.get(config.getOlapClientTickTime(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    } catch (ExecutionException e) {
                        throw Exceptions.rawIOException(e.getCause());
                    } catch (TimeoutException e) {
                        /*
                         * A TimeoutException just means that tickTime expired. That's okay, we just stick our
                         * head up and make sure that the client is still operating
                         */
                    }
                }
                StructType externalSchema = result.getSchema();
                boolean isCsv = storedAs.equalsIgnoreCase("t");
                checkSchema(externalSchema, activation, isCsv);
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private void createConstraints(Activation activation) throws StandardException {
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
    }

    private ColumnDescriptor[] createColumnDescriptorArray(DataDictionary dd, TableDescriptor td,
                                                           List<String> pkColumnNames, int index) {
        ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnInfo.length];
        ColumnDescriptor columnDescriptor;
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
                        columnInfo[ix].partitionPosition,
                        (byte)0
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
        return cdlArray;
    }

    private void checkSchema(StructType externalSchema, Activation activation,
                             boolean isCsv) throws StandardException {
        if(isCsv && externalSchema.fields().length == 0)
        {
            // CSV table is empty, don't check
            return;
        }

        ExecRow template = new ValueRow(columnInfo.length);
        DataValueDescriptor[] dvds = template.getRowArray();
        for (int i = 0; i < columnInfo.length; ++i) {
            dvds[i] = columnInfo[i].dataType.getNull();
        }

        //Make sure we have the same amount of attributes in the definition compared to the external file
        if (externalSchema.fields().length != template.length()) {
            throw StandardException.newException(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE,
                    template.length(), externalSchema.fields().length,
                    location, ExternalTableUtils.getSuggestedSchema(externalSchema));
        }


        int nPartitionColumns = 0;
        for (ColumnInfo column:columnInfo) {
            if (column.partitionPosition >= 0)
                nPartitionColumns++;
        }

        ExecRow nonPartitionColumns = new ValueRow(template.nColumns()-nPartitionColumns);
        DataValueDescriptor[] dvds_nonpart = nonPartitionColumns.getRowArray();
        String[] name_nonpart = new String[template.nColumns()-nPartitionColumns];

        ExecRow partitionColumns = new ValueRow(nPartitionColumns);
        DataValueDescriptor[] dvds_part = partitionColumns.getRowArray();
        String[] name_part = new String[nPartitionColumns];

        int index1 = 0;
        for(int i = 0; i < columnInfo.length; ++i) {
            if (columnInfo[i].partitionPosition >=0) {
                dvds_part[columnInfo[i].partitionPosition] = dvds[i];
                name_part[columnInfo[i].partitionPosition] = columnInfo[i].name;
            }
            else {
                dvds_nonpart[index1] = dvds[i];
                name_nonpart[index1] = columnInfo[i].name;
                index1++;
            }
        }

        // csv will infer all columns as strings currently, so don't do a check for types
        if( !isCsv ) {
            // Compare non-partition columns
            for (int i = 0; i < nonPartitionColumns.nColumns(); ++i) {
                //compare the data type
                StructField externalField = externalSchema.fields()[i];
                StructField definedField = nonPartitionColumns.schema().fields()[i];
                if (!definedField.dataType().equals(externalField.dataType())) {
                    if (!supportAvroDateToString(storedAs, externalField, definedField)) {
                        throw StandardException.newException(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES,
                                name_nonpart[i], ExternalTableUtils.getSqlTypeName(definedField.dataType()),
                                externalField.name(), ExternalTableUtils.getSqlTypeName(externalField.dataType()),
                                location, ExternalTableUtils.getSuggestedSchema(externalSchema));
                    }
                }
            }
        }

        // Compare partition columns
        for (int i = 0; i < partitionColumns.nColumns(); ++i) {
            StructField externalField = externalSchema.fields()[nonPartitionColumns.nColumns() + i];
            StructField definedField = partitionColumns.schema().fields()[i];
            if (!definedField.dataType().equals(externalField.dataType())) {
                if (!supportAvroDateToString(storedAs,externalField,definedField)) {
                    Object[] objects = new Object[]{
                            name_part[i], ExternalTableUtils.getSqlTypeName(definedField.dataType()),
                            externalField.name(), ExternalTableUtils.getSqlTypeName(externalField.dataType()),
                            location, ExternalTableUtils.getSuggestedSchema(externalSchema) };
                    SQLWarning warning = StandardException.newWarning(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES, objects);
                    activation.addWarning(warning);
                }
            }
        }

    }
    private boolean supportAvroDateToString(String storedAs, StructField externalField, StructField definedField){
        return storedAs.toLowerCase().equals("a") && externalField.dataType().equals(DataTypes.StringType) && definedField.dataType().equals(DataTypes.DateType);
    }

    protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td,
                                                                    long conglomId, SchemaDescriptor sd,
                                                                    DataDescriptorGenerator ddg) throws StandardException{
        return ddg.newConglomerateDescriptor(conglomId, null, false, null, false, null, td.getUUID(), sd.getUUID());
    }

    public String getScopeName() {
        return String.format("Create Table %s", tableName);
    }

    private byte[][] calculateSplitKeys(Activation activation,
                                        ColumnOrdering[] columnOrderings,
                                        ColumnInfo[] columnInfo) throws IOException, StandardException{

        Map<String, byte[]> keysMap = new HashMap<>();
        DataSetProcessor dsp = EngineDriver.driver().processorFactory().localProcessor(null,null);
        DataSet<String> text = dsp.readTextFile(splitKeyPath);
        if (isLogicalKey) {
            int[] pkCols = new int[columnOrderings.length];
            for (int i = 0; i < columnOrderings.length; ++i) {
                pkCols[i] = columnOrderings[i].getColumnId();
            }
            int[] allFormatIds = new int[columnInfo.length];
            for (int i = 0; i < columnInfo.length; ++i) {
                allFormatIds[i] = columnInfo[i].dataType.getNull().getTypeFormatId();
            }

            int[] pkFormatIds = new int[pkCols.length];
            for (int i = 0; i < pkCols.length; ++i) {
                pkFormatIds[i] = allFormatIds[pkCols[i]];
            }

            OperationContext operationContext = dsp.createOperationContext(activation);
            ExecRow execRow = WriteReadUtils.getExecRowFromTypeFormatIds(pkFormatIds);
            DataSet<ExecRow> dataSet = text.flatMap(new FileFunction(characterDelimiter, columnDelimiter, execRow,
                    null, timeFormat, dateFormat, timestampFormat, false, operationContext), true);
            List<ExecRow> rows = dataSet.collect();
            DataHash encoder = getEncoder(execRow);
            for (ExecRow row : rows) {
                encoder.setRow(row);
                byte[] bytes = encoder.encode();
                String s = Bytes.toHex(bytes);
                if (keysMap.get(s) == null) {
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "execRow = %s, splitKey = %s", execRow,
                                Bytes.toStringBinary(bytes));
                    }
                    keysMap.put(s, bytes);
                }
            }
        }
        else {
            List<String> physicalKeys = text.collect();
            for (String key : physicalKeys) {
                byte[] bytes = Bytes.toBytesBinary(key);
                String s = Bytes.toHex(bytes);
                if (keysMap.get(s) == null) {
                    keysMap.put(s, bytes);
                }
            }
        }
        byte[][] splitKeys = new byte[keysMap.size()][];
        splitKeys = keysMap.values().toArray(splitKeys);
        return splitKeys;
    }

    private DataHash getEncoder(ExecRow execRow) {
        DescriptorSerializer[] serializers= VersionedSerializers
                .forVersion(SYSTABLESRowFactory.CURRENT_TABLE_VERSION, false)
                .getSerializers(execRow.getRowArray());
        int[] rowColumns = IntArrays.count(execRow.nColumns());
        boolean[] sortOrder = new boolean[execRow.nColumns()];
        for (int i = 0; i < sortOrder.length; i++) {
            sortOrder[i] = true;
        }
        DataHash dataHash = BareKeyHash.encoder(rowColumns, sortOrder, serializers);
        return dataHash;
    }
}

