package com.splicemachine.derby.utils;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSTATISTICSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.stats.SimpleOverheadManagedPartitionStatistics;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.log4j.Logger;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.IndexLister;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.StatementTablePermission;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import javax.annotation.Nullable;

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
        schema = SpliceUtils.validateSchema(schema);
        table = SpliceUtils.validateTable(table);
        columnName = SpliceUtils.validateColumnName(columnName);
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
                    conn.getLanguageConnection().getDataDictionary().setCollectStats(conn.getLanguageConnection().getTransactionCompile(),td.getUUID(),columnName,false);
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
        schema = SpliceUtils.validateSchema(schema);
        table = SpliceUtils.validateTable(table);
        columnName = SpliceUtils.validateColumnName(columnName);
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn, schema, table);
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                if (descriptor.getColumnName().equalsIgnoreCase(columnName)) {
                    DataTypeDescriptor type = descriptor.getType();
                    if (!ColumnDescriptor.allowsStatistics(type))
                        throw ErrorState.LANG_COLUMN_STATISTICS_NOT_POSSIBLE.newException(columnName, type
                            .getTypeName());
                    descriptor.setCollectStatistics(true);
                    conn.getLanguageConnection().getDataDictionary().setCollectStats(conn.getLanguageConnection().getTransactionCompile(), td.getUUID(), columnName, true);
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
        new GenericColumnDescriptor("regionsCollected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("tasksExecuted", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("rowsCollected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    @SuppressWarnings("unused")
    public static void COLLECT_SCHEMA_STATISTICS(String schema, boolean staleOnly, ResultSet[] outputResults) throws
        SQLException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        List<ExecRow> rows = Lists.newArrayList();
        try {
            if (schema == null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException(); //TODO -sf- change this to proper SCHEMA
                // error?
            schema = schema.toUpperCase();

            LanguageConnectionContext lcc = conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();

            SchemaDescriptor sd = getSchemaDescriptor(schema, lcc, dd);
            //get a list of all the TableDescriptors in the schema
            List<TableDescriptor> tds = getAllTableDescriptors(sd, conn);
            if (tds.isEmpty()) {
                // No point in continuing with empty TableDescriptor list, possible NPE
                return;
            }
            authorize(tds);
            ExecRow templateOutputRow = buildOutputTemplateRow();
            TransactionController transactionExecute = lcc.getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();

            // Create the Dataset.  This needs to stay in a dataset for parallel execution (very important).
            boolean first = true;
            DataSet<ExecRow> dataSet = null;
            for (TableDescriptor td : tds) {
                if (first) {
                    dataSet = collectTableStatistics(td, txn, conn);
                    first = false;
                } else
                    dataSet.union(collectTableStatistics(td, txn, conn));
            }
            IteratorNoPutResultSet resultsToWrap = wrapResults(conn,
                    displayTableStatistics(dataSet,dd));
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
        Activation activation = conn.getLanguageConnection().getLastActivation();
        List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
        for (TableDescriptor tableDescriptor : tableDescriptorList) {
            StatementTablePermission key = null;
            try {
                key = new StatementTablePermission(tableDescriptor.getUUID(), Authorizer.INSERT_PRIV);
                requiredPermissionsList.add(key);
                activation.getLanguageConnectionContext().getAuthorizer().authorize(activation, 1);
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

    @SuppressWarnings("UnusedDeclaration")
    public static void COLLECT_TABLE_STATISTICS(String schema,
                                                String table,
                                                boolean staleOnly,
                                                ResultSet[] outputResults) throws SQLException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            schema = SpliceUtils.validateSchema(schema);
            table = SpliceUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            List<TableDescriptor> tableDescriptorList = new ArrayList<>();
            tableDescriptorList.add(tableDesc);
            authorize(tableDescriptorList);
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            ExecRow outputRow = buildOutputTemplateRow();
            TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
            IteratorNoPutResultSet resultsToWrap = wrapResults(conn,
                    displayTableStatistics(collectTableStatistics(tableDesc, txn, conn),dd));
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
            long[] conglomIds = SpliceAdmin.getConglomNumbers(conn, schema, null);
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) tc).getRawTransaction().getActiveStateTxn();

            for (int i =0; i< conglomIds.length;i++) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"Dropping conglomerate %d",conglomIds[i]);
                dd.deleteColumnStatistics(conglomIds[i],tc);
                dd.deleteTableStatistics(conglomIds[i],tc);
            }
            notifyServersClearStatsCache(txn, conglomIds);
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
            schema = SpliceUtils.validateSchema(schema);
            table = SpliceUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            long[] conglomIds = SpliceAdmin.getConglomNumbers(conn, schema, table);
            assert conglomIds != null && conglomIds.length > 0;

            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) tc).getRawTransaction().getActiveStateTxn();
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            for (int i =0; i< conglomIds.length;i++) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"Dropping conglomerate %d",conglomIds[i]);
                dd.deleteColumnStatistics(conglomIds[i],tc);
                dd.deleteTableStatistics(conglomIds[i],tc);
            }
            notifyServersClearStatsCache(txn, conglomIds);
            SpliceLogUtils.debug(LOG, "Done dropping statistics for table %s.", table);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            if (conn != null) conn.close();
        }
    }

    private static void notifyServersClearStatsCache(TxnView txn, long[] conglomIds) throws SQLException {
        // Notify: listener on each region server will invalidate cache
        // If this fails or times out, rethrow the exception as usual
        // which will roll back the deletions.

        SpliceLogUtils.debug(LOG, "Notifying region servers to clear statistics caches.");
        String changeId = null;

        DDLChange change = ProtoUtil.clearStats(txn.getTxnId(),conglomIds);
        try {
            changeId = DDLCoordinationFactory.getController().notifyMetadataChange(change);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            try {
                if (changeId != null) {
                    // Finish (nothing more than clearing zk nodes)
                    DDLCoordinationFactory.getController().finishMetadataChange(changeId);
                }
                SpliceLogUtils.debug(LOG, "Notified servers are finished clearing statistics caches.");
            } catch (StandardException e) {
                SpliceLogUtils.error(LOG, "Error finishing dropping statistics", e);
            }
        }
    }
    
    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static DataSet<ExecRow> collectTableStatistics(TableDescriptor table,
                                                         TxnView txn,
                                                         EmbedConnection conn) throws StandardException, ExecutionException {

       return collectBaseTableStatistics(table, txn, conn);
    }

    private static DataSet collectBaseTableStatistics(TableDescriptor table,
                                                             TxnView txn,
                                                             EmbedConnection conn) throws StandardException, ExecutionException {
        long heapConglomerateId = table.getHeapConglomerateId();
        Activation activation = conn.getLanguageConnection().getLastActivation();
        DataSetProcessor dsp = StreamUtils.sparkDataSetProcessor;
        StreamUtils.setupSparkJob(dsp, activation, "collecting table statistics", "admin");
        TableScannerBuilder tableScannerBuilder = createTableScannerBuilder(conn, table, txn);
        DataSet dataSet = (DataSet)dsp.getTableScanner(activation, tableScannerBuilder, (new Long(heapConglomerateId).toString()));
        return dataSet;
    }

    private static void invalidateStatisticsCache(long conglomId) throws ExecutionException{
        if(!StatisticsStorage.isStoreRunning()) return; //nothing to invalidate
        StatisticsStorage.getPartitionStore().invalidateCachedStatistics(conglomId);
    }

    private static Scan createScan (TxnView txn) {
        Scan scan= SpliceUtils.createScan(txn, false);
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scan.setStartRow(new byte[0]);
        scan.setStopRow(new byte[0]);
        scan.setCacheBlocks(false);
        return scan;
    }

    public static int[] getFormatIds(EmbedConnection conn, long columnStatsConglomId) throws StandardException{
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(columnStatsConglomId);
        int[] formatIds = conglomerate.getFormat_ids();
        return formatIds;
    }

    private static TableScannerBuilder createTableScannerBuilder(EmbedConnection conn,
                                                              TableDescriptor table,
                                                              TxnView txn) throws StandardException {

        List<ColumnDescriptor> colsToCollect = getCollectedColumns(table);
        ExecRow row = new ValueRow(colsToCollect.size());
        int[] execRowFormatIds = new int[colsToCollect.size()];
        BitSet accessedColumns = new BitSet(table.getNumberOfColumns());
        int outputCol = 0;
        int[] columnPositionMap = new int[table.getNumberOfColumns()];
        Arrays.fill(columnPositionMap, -1);
        int[] allColumnLengths = new int[table.getNumberOfColumns()];
        for (ColumnDescriptor descriptor : colsToCollect) {
            accessedColumns.set(descriptor.getPosition() - 1);
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
        Scan scan = createScan(txn);
        return new TableScannerBuilder()
                .transaction(txn)
                .execRowTypeFormatIds(execRowFormatIds)
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
                .columnPositionMap(columnPositionMap);
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

    public static ExecRow generateRowFromStats(long conglomId, String partitionId,SimpleOverheadManagedPartitionStatistics statistics) throws StandardException {
        ExecRow row = new ValueRow(SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.TIMESTAMP,new SQLTimestamp(new Timestamp(System.currentTimeMillis())));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STALENESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.INPROGRESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT,new SQLLongint(statistics.rowCount()));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE,new SQLLongint(statistics.totalSize()));
        row.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH,new SQLInteger(statistics.avgRowWidth()));
        return row;
    }

    public static ExecRow generateRowFromStats(long conglomId, String regionId, int columnId, ColumnStatistics columnStatistics) throws StandardException {
        ExecRow row = new ValueRow(SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.CONGLOMID,new SQLLongint(new Long(conglomId)));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.PARTITIONID,new SQLVarchar(regionId));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.COLUMNID,new SQLInteger(columnId));
        row.setColumn(SYSCOLUMNSTATISTICSRowFactory.DATA,new UserType(columnStatistics));
        return row;
    }

    public static ExecRow generateOutputRow(String schemaName, String tableName, String partitionName, ExecRow partitionRow) throws StandardException {
        ExecRow row = new ValueRow(5);
        row.setColumn(1,new SQLVarchar(schemaName));
        row.setColumn(2,new SQLVarchar(tableName));
        row.setColumn(3,new SQLVarchar(partitionName));
        row.setColumn(4,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT));
        row.setColumn(5,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE));
        return row;
    }


    public static Iterable displayTableStatistics(DataSet dataSet, final DataDictionary dataDictionary) {
        return new ControlDataSet(dataSet).flatMap(new SpliceFlatMapFunction<SpliceOperation,ExecRow,ExecRow>() {
            @Override
            public Iterable<ExecRow> call(ExecRow execRow) throws Exception {
                if (execRow.nColumns() == SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT) {
                    // Write Data...
                    return Collections.EMPTY_LIST;
                } else {
                    // Write data...
                    return Collections.EMPTY_LIST;
                }

            }
        });
    }
}