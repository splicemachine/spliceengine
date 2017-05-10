/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.stats.ItemStatistics;
import org.spark_project.guava.base.Function;
import com.splicemachine.derby.utils.stats.DistributedStatsCollection;
import com.splicemachine.derby.utils.stats.StatsResult;
import org.spark_project.guava.collect.FluentIterable;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSTATISTICSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsAdmin extends BaseAdminProcedures {
    private static final Logger LOG = Logger.getLogger(StatisticsAdmin.class);
    public static final String TABLEID_FROM_SCHEMA = "select tableid from sys.systables t where t.schemaid = ?";

    @SuppressWarnings("UnusedDeclaration")
    public static void DISABLE_COLUMN_STATISTICS(String schema,
                                                 String table,
                                                 String columnName) throws SQLException {
        schema = EngineUtils.validateSchema(schema);
        table = EngineUtils.validateTable(table);
        columnName = EngineUtils.validateColumnName(columnName);
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn, schema, table);
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                if (descriptor.getColumnName().equalsIgnoreCase(columnName)) {
                    //need to make sure it's not a pk or indexed column
                    ensureNotKeyed(descriptor, td);
                    descriptor.setCollectStatistics(false);
                    LanguageConnectionContext languageConnection=conn.getLanguageConnection();
                    TransactionController transactionCompile=languageConnection.getTransactionCompile();
                    transactionCompile.elevate("dictionary");
                    languageConnection.getDataDictionary().setCollectStats(transactionCompile, td.getUUID(), columnName, false);
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName, schema + "." + table);
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static void ENABLE_COLUMN_STATISTICS(String schema,
                                                String table,
                                                String columnName) throws SQLException {
        schema = EngineUtils.validateSchema(schema);
        table = EngineUtils.validateTable(table);
        columnName = EngineUtils.validateColumnName(columnName);
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn, schema, table);
/*            if (td!=null && td.getTableType()==TableDescriptor.EXTERNAL_TYPE) {
                throw StandardException.newException(
                        com.splicemachine.db.iapi.reference.SQLState.EXTERNAL_TABLES_NO_STATS,td.getName());
            }
*/
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                if (descriptor.getColumnName().equalsIgnoreCase(columnName)) {
                    DataTypeDescriptor type = descriptor.getType();
                    if (!ColumnDescriptor.allowsStatistics(type))
                        throw ErrorState.LANG_COLUMN_STATISTICS_NOT_POSSIBLE.newException(columnName, type.getTypeName());
                    descriptor.setCollectStatistics(true);
                    LanguageConnectionContext languageConnection=conn.getLanguageConnection();
                    TransactionController transactionCompile=languageConnection.getTransactionCompile();
                    transactionCompile.elevate("dictionary");
                    languageConnection.getDataDictionary().setCollectStats(transactionCompile, td.getUUID(), columnName, true);
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName, schema + "." + table);

        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static final ResultColumnDescriptor[] COLLECTED_STATS_OUTPUT_COLUMNS = new GenericColumnDescriptor[]{
        new GenericColumnDescriptor("schemaName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("tableName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("partition", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("rowsCollected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("partitionSize", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    @SuppressWarnings("unused")
    public static void COLLECT_SCHEMA_STATISTICS(String schema, boolean staleOnly, ResultSet[] outputResults) throws
        SQLException {
        EmbedConnection conn = (EmbedConnection)getDefaultConn();
//        List<ExecRow> rows = Lists.newArrayList();
        try {
            if (schema == null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException(); //TODO -sf- change this to proper SCHEMA
                // error?
            schema = EngineUtils.validateSchema(schema);

            LanguageConnectionContext lcc = conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();
            dd.startWriting(lcc);

            /* Invalidate dependencies remotely. */

            TransactionController tc = lcc.getTransactionExecute();

            SchemaDescriptor sd = getSchemaDescriptor(schema, lcc, dd);
            //get a list of all the TableDescriptors in the schema
            List<TableDescriptor> tds = getAllTableDescriptors(sd, conn);
            if (tds.isEmpty()) {
                // No point in continuing with empty TableDescriptor list, possible NPE
                return;
            }
            authorize(tds);
            TransactionController transactionExecute = lcc.getTransactionExecute();
            transactionExecute.elevate("statistics");
            dropTableStatistics(tds,dd,tc);
            ddlNotification(tc,tds);
//            ExecRow templateOutputRow = buildOutputTemplateRow();
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();

            // Create the Dataset.  This needs to stay in a dataset for parallel execution (very important).
            DataSet<ExecRow> dataSet = null;

            HashMap<Long,Pair<String,String>> display = new HashMap<>();
            List<Future<StatsResult>> futures = new ArrayList(tds.size());
            for (TableDescriptor td : tds) {
                display.put(td.getHeapConglomerateId(),Pair.newPair(schema,td.getName()));
                futures.add(collectTableStatistics(td, txn, conn));
            }
            IteratorNoPutResultSet resultsToWrap = wrapResults(conn,
            displayTableStatistics(futures,dd,transactionExecute,display));
            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } catch (ExecutionException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        }
    }

    private static void authorize(List<TableDescriptor> tableDescriptorList) throws SQLException, StandardException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        Authorizer authorizer=lcc.getAuthorizer();
        Activation activation = lcc.getActivationCount()>0?lcc.getLastActivation():null;
        if(activation==null){
            //TODO -sf- this can happen sometimes for some reason
            for(TableDescriptor td : tableDescriptorList){
                authorizer.authorize(Authorizer.INSERT_PRIV);
            }
            return;
        }
        List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
        for (TableDescriptor tableDescriptor : tableDescriptorList) {
            StatementTablePermission key = null;
            try {

                key = new StatementTablePermission(
                        tableDescriptor.getSchemaDescriptor().getUUID(),
                        tableDescriptor.getUUID(),
                        Authorizer.INSERT_PRIV);

                requiredPermissionsList.add(key);
                lcc.getAuthorizer().authorize(activation, 1);
            } catch (StandardException e) {
                if (e.getSqlState().compareTo(SQLState.AUTH_NO_TABLE_PERMISSION) == 0) {
                    throw StandardException.newException(
                        com.splicemachine.db.iapi.reference.SQLState.AUTH_NO_TABLE_PERMISSION_FOR_ANALYZE,
                        lcc.getCurrentUserId(activation),
                        "INSERT",
                        tableDescriptor.getSchemaName(),
                        tableDescriptor.getName());
                } else throw e;
            } finally {
                if (key != null) {
                    requiredPermissionsList.remove(key);
                }
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    public static void COLLECT_TABLE_STATISTICS(String schema,
                                                String table,
                                                boolean staleOnly,
                                                ResultSet[] outputResults) throws SQLException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            schema = EngineUtils.validateSchema(schema);
            table = EngineUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            /*
            if (tableDesc!=null && tableDesc.getTableType()==TableDescriptor.EXTERNAL_TYPE) {
                throw StandardException.newException(
                        com.splicemachine.db.iapi.reference.SQLState.EXTERNAL_TABLES_NO_STATS,tableDesc.getName());
            }
            */
            List<TableDescriptor> tds = Collections.singletonList(tableDesc);
            authorize(tds);
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            dd.startWriting(conn.getLanguageConnection());
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            dropTableStatistics(tds,dd,tc);
            ddlNotification(tc, tds);
            TxnView txn = ((SpliceTransactionManager) tc).getRawTransaction().getActiveStateTxn();
            HashMap<Long,Pair<String,String>> display = new HashMap<>();
            display.put(tableDesc.getHeapConglomerateId(),Pair.newPair(schema,table));
            IteratorNoPutResultSet resultsToWrap = wrapResults(
                conn,
                displayTableStatistics(Lists.newArrayList(
                    collectTableStatistics(tableDesc, txn, conn)
                ),
                dd, tc, display));
            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } catch (ExecutionException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        }
    }

    public static void DROP_SCHEMA_STATISTICS(String schema) throws SQLException {
        EmbedConnection conn = (EmbedConnection) getDefaultConn();
        try {
            if (schema == null)
                throw ErrorState.LANG_SCHEMA_DOES_NOT_EXIST.newException();
            schema = schema.toUpperCase();
            LanguageConnectionContext lcc = conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();
            SchemaDescriptor sd = getSchemaDescriptor(schema, lcc, dd);
            List<TableDescriptor> tds = getAllTableDescriptors(sd, conn);
            authorize(tds);
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            dropTableStatistics(tds,dd,tc);
            ddlNotification(tc,tds);
            SpliceLogUtils.debug(LOG, "Done dropping statistics for schema %s.", schema);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            if (conn != null) conn.close();
        }
    }

    public static void DROP_TABLE_STATISTICS(String schema, String table) throws SQLException {
        EmbedConnection conn = (EmbedConnection) getDefaultConn();
        try {
            schema = EngineUtils.validateSchema(schema);
            table = EngineUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            List<TableDescriptor> tds = Collections.singletonList(tableDesc);
            dropTableStatistics(tds,dd,tc);
            ddlNotification(tc,tds);
            SpliceLogUtils.debug(LOG, "Done dropping statistics for table %s.", table);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            if (conn != null) conn.close();
        }
    }

    private static void ddlNotification(TransactionController tc,  List<TableDescriptor> tds) throws StandardException {
        DDLChange ddlChange = ProtoUtil.alterStats(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),tds);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static Future<StatsResult> collectTableStatistics(TableDescriptor table,
                                                             TxnView txn,
                                                             EmbedConnection conn) throws StandardException, ExecutionException {

       return collectBaseTableStatistics(table, txn, conn);
    }

    private static Future<StatsResult> collectBaseTableStatistics(TableDescriptor table,
                                                                 TxnView txn,
                                                                 EmbedConnection conn) throws StandardException, ExecutionException {
        long heapConglomerateId = table.getHeapConglomerateId();
        Activation activation = conn.getLanguageConnection().getLastActivation();
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();

        ScanSetBuilder ssb = dsp.newScanSet(null,Long.toString(heapConglomerateId));
        ScanSetBuilder scanSetBuilder = createTableScanner(ssb,conn,table,txn);
        String scope = getScopeName(table);

        try {
            return EngineDriver.driver().getOlapClient().submit(new DistributedStatsCollection(scanSetBuilder, scope, activation.getLanguageConnectionContext().getCurrentUserId(activation)));
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    private static final String getScopeName(TableDescriptor td) {
        return String.format(OperationContext.Scope.COLLECT_STATS.displayName(), td.getName());
    }

    private static DataScan createScan (TxnView txn) {
        DataScan scan=SIDriver.driver().getOperationFactory().newDataScan(txn);
        scan.returnAllVersions(); //make sure that we read all versions of the data
        return scan.startKey(new byte[0]).stopKey(new byte[0]);
    }

    public static int[] getFormatIds(EmbedConnection conn, long columnStatsConglomId) throws StandardException{
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(columnStatsConglomId);
        return conglomerate.getFormat_ids();
    }

    private static ScanSetBuilder createTableScanner(ScanSetBuilder builder,
                                                     EmbedConnection conn,
                                                     TableDescriptor table,
                                                     TxnView txn) throws StandardException{

        List<ColumnDescriptor> colsToCollect = getCollectedColumns(table);
        ExecRow row = new ValueRow(colsToCollect.size());
        int[] execRowFormatIds = new int[colsToCollect.size()];
        BitSet accessedColumns = new BitSet(table.getMaxStorageColumnID());
        int outputCol = 0;
        int[] columnPositionMap = new int[table.getNumberOfColumns()];
        Arrays.fill(columnPositionMap, -1);
        int[] allColumnLengths = new int[table.getMaxStorageColumnID()];
        for (ColumnDescriptor descriptor : colsToCollect) {
            accessedColumns.set(descriptor.getStoragePosition() - 1);
            row.setColumn(outputCol + 1, descriptor.getType().getNull());
            columnPositionMap[outputCol] = descriptor.getPosition();
            execRowFormatIds[outputCol] = descriptor.getType().getNull().getFormat().getStoredFormatId();
            outputCol++;
            allColumnLengths[descriptor.getPosition() - 1] = descriptor.getType().getMaximumWidth();
        }

        int[] rowDecodingMap = new int[accessedColumns.length()];
        int[] fieldLengths = new int[accessedColumns.length()];
        Arrays.fill(rowDecodingMap, -1);
        outputCol = 0;
        for (int i = accessedColumns.nextSetBit(0); i >= 0; i = accessedColumns.nextSetBit(i + 1)) {
            rowDecodingMap[i] = outputCol;
            fieldLengths[outputCol] = allColumnLengths[i];
            outputCol++;
        }
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(table.getHeapConglomerateId());
        boolean[] keyColumnSortOrder = conglomerate.getAscDescInfo();
        int[] keyColumnEncodingOrder = conglomerate.getColumnOrdering();
        int[] formatIds = conglomerate.getFormat_ids();
        int[] keyColumnTypes = null;
        int[] keyDecodingMap = null;
        FormatableBitSet collectedKeyColumns = null;
        if (keyColumnEncodingOrder != null) {
            keyColumnTypes = new int[keyColumnEncodingOrder.length];
            keyDecodingMap = new int[keyColumnEncodingOrder.length];
            Arrays.fill(keyDecodingMap, -1);
            collectedKeyColumns = new FormatableBitSet(table.getNumberOfColumns());
            for (int i = 0; i < keyColumnEncodingOrder.length; i++) {
                int keyColumn = keyColumnEncodingOrder[i];
                keyColumnTypes[i] = formatIds[keyColumn];
                if (accessedColumns.get(keyColumn)) {
                    collectedKeyColumns.set(i);
                    keyDecodingMap[i] = rowDecodingMap[keyColumn];
                    rowDecodingMap[keyColumn] = -1;
                }
            }
        }
        DataScan scan = createScan(txn);
        ExecRow rowTemplate = new ValueRow(execRowFormatIds.length);
        DataValueDescriptor[] dvds = rowTemplate.getRowArray();
        DataValueFactory dataValueFactory=conn.getLanguageConnection().getDataValueFactory();
        for(int i=0;i<execRowFormatIds.length;i++){
            dvds[i] = dataValueFactory.getNull(execRowFormatIds[i],-1);
        }
        return builder.transaction(txn)
                .metricFactory(Metrics.basicMetricFactory())
                .template(rowTemplate)
                .scan(scan)
                .rowDecodingMap(rowDecodingMap)
                .keyColumnEncodingOrder(keyColumnEncodingOrder)
                .keyColumnSortOrder(keyColumnSortOrder)
                .keyColumnTypes(keyColumnTypes)
                .keyDecodingMap(keyDecodingMap)
                .baseTableConglomId(table.getHeapConglomerateId())
                .accessedKeyColumns(collectedKeyColumns)
                .tableVersion(table.getVersion())
                .fieldLengths(fieldLengths)
                .columnPositionMap(columnPositionMap)
                .oneSplitPerRegion(true)
                .storedAs(table.getStoredAs())
                .location(table.getLocation())
                .compression(table.getCompression())
                .delimited(table.getDelimited())
                .lines(table.getLines())
                .escaped(table.getEscaped())
                ;
    }

    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn, Iterable<ExecRow> rows) throws
        StandardException {
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, COLLECTED_STATS_OUTPUT_COLUMNS,
                                                                          lastActivation);
        resultsToWrap.openCore();
        return resultsToWrap;
    }

    private static ExecRow buildOutputTemplateRow() throws StandardException {
        ExecRow outputRow = new ValueRow(COLLECTED_STATS_OUTPUT_COLUMNS.length);
        DataValueDescriptor[] dvds = new DataValueDescriptor[COLLECTED_STATS_OUTPUT_COLUMNS.length];
        for (int i = 0; i < dvds.length; i++) {
            dvds[i] = COLLECTED_STATS_OUTPUT_COLUMNS[i].getType().getNull();
        }
        outputRow.setRowArray(dvds);
        return outputRow;
    }

    private static List<TableDescriptor> getAllTableDescriptors(SchemaDescriptor sd, EmbedConnection conn) throws
        SQLException {
        try (PreparedStatement statement = conn.prepareStatement(TABLEID_FROM_SCHEMA)) {
            statement.setString(1, sd.getUUID().toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
                UUIDFactory uuidFactory = dd.getUUIDFactory();
                List<TableDescriptor> tds = new LinkedList<>();
                while (resultSet.next()) {
                    com.splicemachine.db.catalog.UUID tableId = uuidFactory.recreateUUID(resultSet.getString(1));
                    TableDescriptor tableDescriptor = dd.getTableDescriptor(tableId);
                    /*
                     * We need to filter out views from the TableDescriptor list. Views
                     * are special cases where the number of conglomerate descriptors is 0. We
                     * don't collect statistics for those views
                     */

                    if (tableDescriptor != null && tableDescriptor.getConglomerateDescriptorList().size() > 0) {
                        tds.add(tableDescriptor);
                    }
                }
                return tds;
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static TableDescriptor verifyTableExists(Connection conn, String schema, String table) throws
        SQLException, StandardException {
        LanguageConnectionContext lcc = ((EmbedConnection) conn).getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(schema, lcc, dd);
        TableDescriptor tableDescriptor = dd.getTableDescriptor(table, schemaDescriptor, lcc.getTransactionExecute());
        if (tableDescriptor == null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema + "." + table);

        return tableDescriptor;
    }

    private static SchemaDescriptor getSchemaDescriptor(String schema,
                                                        LanguageConnectionContext lcc,
                                                        DataDictionary dd) throws StandardException {
        SchemaDescriptor schemaDescriptor;
        if (schema ==null || (schemaDescriptor = dd.getSchemaDescriptor(schema, lcc.getTransactionExecute(), true))==null)
            throw ErrorState.LANG_SCHEMA_DOES_NOT_EXIST.newException(schema);
        return schemaDescriptor;
    }

    private static final Comparator<ColumnDescriptor> order = new Comparator<ColumnDescriptor>() {
        @Override
        public int compare(ColumnDescriptor o1, ColumnDescriptor o2) {
            return o1.getPosition() - o2.getPosition();
        }
    };

    private static List<ColumnDescriptor> getCollectedColumns(TableDescriptor td) throws StandardException {
        ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
        List<ColumnDescriptor> toCollect = new ArrayList<>(columnDescriptorList.size());
        /*
         * Get all the enabled statistics columns
         */
        for (ColumnDescriptor columnDescriptor : columnDescriptorList) {
            if (columnDescriptor.collectStatistics())
                toCollect.add(columnDescriptor);
        }
        /*
         * Add in any disabled key columns.
         *
         * We want to collect for all key columns always, because they are very important when
         * comparing index columns. By default, we turn them on when possible, but even if they are disabled
         * for some reason, we should still collect them. Of course, we should also not be able to disable
         * keyed columns, but that's a to-do for now.
         */
        IndexLister indexLister = td.getIndexLister();
        if (indexLister != null) {
            IndexRowGenerator[] distinctIndexRowGenerators = indexLister.getDistinctIndexRowGenerators();
            for (IndexRowGenerator irg : distinctIndexRowGenerators) {
                int[] keyColumns = irg.getIndexDescriptor().baseColumnPositions();
                for (int keyColumn : keyColumns) {
                    for (ColumnDescriptor cd : columnDescriptorList) {
                        if (cd.getPosition() == keyColumn) {
                            if (!toCollect.contains(cd)) {
                                toCollect.add(cd);
                            }
                            break;
                        }
                    }
                }
            }
        }
        Collections.sort(toCollect, order); //sort the columns into adjacent position order
        return toCollect;
    }

    private static void ensureNotKeyed(ColumnDescriptor descriptor, TableDescriptor td) throws StandardException {
        ConglomerateDescriptor heapConglom = td.getConglomerateDescriptor(td.getHeapConglomerateId());
        IndexRowGenerator pkDescriptor = heapConglom.getIndexDescriptor();
        if (pkDescriptor != null && pkDescriptor.getIndexDescriptor() != null) {
            for (int pkCol : pkDescriptor.baseColumnPositions()) {
                if (pkCol == descriptor.getPosition()) {
                    throw ErrorState.LANG_DISABLE_STATS_FOR_KEYED_COLUMN.newException(descriptor.getColumnName());
                }
            }
        }
        IndexLister indexLister = td.getIndexLister();
        if (indexLister != null) {
            for (IndexRowGenerator irg : indexLister.getIndexRowGenerators()) {
                if (irg.getIndexDescriptor() == null) continue;
                for (int col : irg.baseColumnPositions()) {
                    if (col == descriptor.getPosition())
                        throw ErrorState.LANG_DISABLE_STATS_FOR_KEYED_COLUMN.newException(descriptor.getColumnName());
                }
            }
        }
    }

    public static ExecRow generateRowFromStats(long conglomId, String partitionId, long rowCount, long partitionSize, int meanRowWidth) throws StandardException {
        ExecRow row = new ValueRow(SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.TIMESTAMP,new SQLTimestamp(new Timestamp(System.currentTimeMillis())));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STALENESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.INPROGRESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH,new SQLInteger(meanRowWidth));
        return row;
    }

    public static ExecRow generateRowFromStats(long conglomId, String regionId, int columnId, ItemStatistics columnStatistics) throws StandardException {
        ExecRow row = new ValueRow(SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.PARTITIONID,new SQLVarchar(regionId));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.COLUMNID,new SQLInteger(columnId));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.DATA, new UserType(columnStatistics));
        return row;
    }

    public static ExecRow generateOutputRow(String schemaName, String tableName, ExecRow partitionRow) throws StandardException {
        ExecRow row = new ValueRow(5);
        row.setColumn(1,new SQLVarchar(schemaName));
        row.setColumn(2,new SQLVarchar(tableName));
        row.setColumn(3,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID));
        row.setColumn(4,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT));
        row.setColumn(5,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE));
        return row;
    }


    public static Iterable displayTableStatistics(List<Future<StatsResult>> futures, final DataDictionary dataDictionary, final TransactionController tc, final HashMap<Long,Pair<String,String>> displayPair) {
        return FluentIterable.from(futures).transformAndConcat(new Function<Future<StatsResult>, Iterable<ExecRow>>() {
            @Nullable
            @Override
            public Iterable<ExecRow> apply(@Nullable Future<StatsResult> input) {
                try {
                    List<LocatedRow> rows = input.get().getRowList();
                    List<ExecRow> outputList = new ArrayList();
                    for (LocatedRow locatedRow: rows) {
                        ExecRow row = locatedRow.getRow();
                        if (row.nColumns() == SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT) {
                            dataDictionary.addColumnStatistics(row,tc);
                        } else {
                            dataDictionary.addTableStatistics(row, tc);
                            Pair<String,String> pair = displayPair.get(row.getColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID).getLong());
                            outputList.add(generateOutputRow(pair.getFirst(),pair.getSecond(),row));
                        }
                    }
                    return outputList;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static void dropTableStatistics(TableDescriptor td, DataDictionary dd, TransactionController tc) throws StandardException {
        for (ConglomerateDescriptor cd: td.getConglomerateDescriptorList()) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"Dropping conglomerate statistics [%d]",cd.getConglomerateNumber());
            dd.deletePartitionStatistics(cd.getConglomerateNumber(),tc);
        }
    }

    private static void dropTableStatistics(List<TableDescriptor> tds, DataDictionary dd, TransactionController tc) throws StandardException {

        for (TableDescriptor td: tds) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"Dropping Table statistics [%s]",td.getName());
            dropTableStatistics(td,dd,tc);
        }
    }


}