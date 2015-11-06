package com.splicemachine.derby.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
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
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.ddl.ClearStatsCacheDDLDesc;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

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

        if (schema == null)
            schema = getCurrentSchema();
        else
            schema = schema.toUpperCase();
        if (table == null)
            throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
        else
            table = table.toUpperCase();
        if (columnName == null)
            throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_ID.newException());
        else
            columnName = columnName.toUpperCase();
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
                    LanguageConnectionContext languageConnection = conn.getLanguageConnection();
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=false where " +
                                                                     "referenceid = ? and columnname = ?");
                    ps.setString(1, td.getUUID().toString());
                    ps.setString(2, columnName);

                    ps.execute();
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
        if (schema == null)
            schema = getCurrentSchema();
        else
            schema = schema.toUpperCase();
        if (table == null)
            throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
        else
            table = table.toUpperCase();
        if (columnName == null)
            throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_ID.newException());
        else
            columnName = columnName.toUpperCase();

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
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=true where " +
                                                                     "referenceid = ? and columnnumber = ?");
                    ps.setString(1, td.getUUID().toString());
                    ps.setInt(2, descriptor.getPosition());

                    ps.execute();
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
            long start = System.nanoTime();
            for (TableDescriptor td : tds) {
                rows.addAll(collectTableStatistics(td, txn, templateOutputRow, conn, staleOnly));
            }
            long end = System.nanoTime();
            if (LOG.isTraceEnabled()) {
                double timeMicros = (end - start) / 1000d;
                LOG.trace(String.format("Took %.3f micros to submit %d collections", timeMicros, tds.size()));
            }
            IteratorNoPutResultSet results = wrapResults(conn, rows);

            outputResults[0] = new EmbedResultSet40(conn, results, false, null, true);

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
            if (schema == null)
                schema = getCurrentSchema();
            if (table == null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException();

            schema = schema.toUpperCase();
            table = table.toUpperCase();
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            List<TableDescriptor> tableDescriptorList = new ArrayList<>();
            tableDescriptorList.add(tableDesc);
            authorize(tableDescriptorList);

            ExecRow outputRow = buildOutputTemplateRow();
            TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
            List<ExecRow> rows = collectTableStatistics(tableDesc, txn, outputRow, conn, staleOnly);

            IteratorNoPutResultSet resultsToWrap = wrapResults(conn, rows);

            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);

        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } catch (ExecutionException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        }
    }

    public static void DROP_SCHEMA_STATISTICS(String schema) throws SQLException {
        EmbedConnection conn = (EmbedConnection) getDefaultConn();
        PreparedStatement ps = null, ps2 = null;

        try {
            if (schema == null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException();
            schema = schema.toUpperCase();
            LanguageConnectionContext lcc = conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();

            SchemaDescriptor sd = getSchemaDescriptor(schema, lcc, dd);
            List<TableDescriptor> tds = getAllTableDescriptors(sd, conn);
            authorize(tds);

            long[] conglomIds = SpliceAdmin.getConglomNumbers(conn, schema, null);
            assert conglomIds != null && conglomIds.length > 0;

            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) tc).getRawTransaction().getActiveStateTxn();

            // The following 2 queries use existing system indexes: SYSCONGLOMERATES_INDEX1, SYSTABLES_INDEX1.

            ps = conn.prepareStatement("DELETE FROM sys.systablestats WHERE conglomerateid IN (" + SpliceAdmin
                    .getSqlConglomsInSchema() + ")");
            ps.setString(1, schema);
            int deleteCount = ps.executeUpdate();
            SpliceLogUtils.debug(LOG, "Deleted %d rows in sys.systablestats for schema %s", deleteCount, schema);

            ps2 = conn.prepareStatement("DELETE FROM sys.syscolumnstats WHERE conglom_id IN (" + SpliceAdmin
                .getSqlConglomsInSchema() + ")");
            ps2.setString(1, schema);
            deleteCount = ps2.executeUpdate();
            SpliceLogUtils.debug(LOG, "Deleted %d rows in sys.syscolumnstats for schema %s", deleteCount, schema);

            notifyServersClearStatsCache(txn, conglomIds);

            SpliceLogUtils.debug(LOG, "Done dropping statistics for schema %s.", schema);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            if (ps != null) ps.close();
            if (ps2 != null) ps2.close();
            if (conn != null) conn.close();
        }
    }

    public static void DROP_TABLE_STATISTICS(String schema, String table) throws SQLException {
        EmbedConnection conn = (EmbedConnection) getDefaultConn();
        PreparedStatement ps = null, ps2 = null;

        try {
            if (schema == null)
                schema = "SPLICE";
            if (table == null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException();

            schema = schema.toUpperCase();
            table = table.toUpperCase();
            verifyTableExists(conn, schema, table);
            long[] conglomIds = SpliceAdmin.getConglomNumbers(conn, schema, table);
            assert conglomIds != null && conglomIds.length > 0;

            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) tc).getRawTransaction().getActiveStateTxn();

            String inConglomIds = Joiner.on(",").join(Longs.asList(conglomIds));
            ps = conn.prepareStatement("DELETE FROM sys.systablestats WHERE conglomerateid IN (" + inConglomIds + ")");
            int deleteCount = ps.executeUpdate();
            SpliceLogUtils.debug(LOG, "Deleted %d rows in sys.systablestats for table %s", deleteCount, table);

            ps2 = conn.prepareStatement("DELETE FROM sys.syscolumnstats WHERE conglom_id IN (" + inConglomIds + ")");
            deleteCount = ps2.executeUpdate();
            SpliceLogUtils.debug(LOG, "Deleted %d rows in sys.syscolumnstats for table %s", deleteCount, table);

            notifyServersClearStatsCache(txn, conglomIds);

            SpliceLogUtils.debug(LOG, "Done dropping statistics for table %s.", table);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } finally {
            if (ps != null) ps.close();
            if (ps2 != null) ps2.close();
            if (conn != null) conn.close();
        }
    }

    private static void notifyServersClearStatsCache(TxnView txn, long[] conglomIds) throws SQLException {
        // Notify: listener on each region server will invalidate cache
        // If this fails or times out, rethrow the exception as usual
        // which will roll back the deletions.

        SpliceLogUtils.debug(LOG, "Notifying region servers to clear statistics caches.");
        String changeId = null;
        DDLChange change = new DDLChange(txn, DDLChangeType.CLEAR_STATS_CACHE);
        change.setTentativeDDLDesc(new ClearStatsCacheDDLDesc(conglomIds));
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
    private static List <ExecRow> collectTableStatistics(TableDescriptor table,
                                                         TxnView txn,
                                                         ExecRow templateOutputRow,
                                                         EmbedConnection conn,
                                                         boolean staleOnly) throws StandardException, ExecutionException {

        List<ExecRow> tableStatisticsrows = collectBaseTableStatistics(table, txn, templateOutputRow, conn, staleOnly);
        List<ExecRow> indexStatisticsRows = Lists.newArrayList();

        long heapConglomerateId = table.getHeapConglomerateId();
        List<ColumnDescriptor> colsToCollect = getCollectedColumns(table);
        //get the indices for the table
        IndexLister indexLister = table.getIndexLister();
        IndexRowGenerator[] indexGenerators;
        if (indexLister != null) {
            indexGenerators = indexLister.getDistinctIndexRowGenerators();
        } else
            indexGenerators = new IndexRowGenerator[]{};

        // collect statistics for all indexes
        if (indexGenerators.length > 0) {
            //we know this is safe because we've actually already checked for it
            @SuppressWarnings("ConstantConditions") long[] indexConglomIds = indexLister
                    .getDistinctIndexConglomerateNumbers();
            String[] distinctIndexNames = indexLister.getDistinctIndexNames();
            for (int i = 0; i < indexGenerators.length; i++) {
                IndexRowGenerator irg = indexGenerators[i];
                long indexConglomId = indexConglomIds[i];
                Collection<HRegionInfo> indexRegions = getCollectedRegions(conn, indexConglomId, staleOnly);
                indexStatisticsRows.addAll(collectIndexStatistics(conn, irg, indexConglomId, distinctIndexNames[i], table,
                        colsToCollect, templateOutputRow, txn, staleOnly));
            }
        }
        indexStatisticsRows.addAll(tableStatisticsrows);
        return indexStatisticsRows;
    }

    private static List<ExecRow> collectIndexStatistics(EmbedConnection conn,
                                                        IndexRowGenerator irg,
                                                        long indexConglomerateId,
                                                        String indexName,
                                                        TableDescriptor table,
                                                        List<ColumnDescriptor> columnsToCollect,
                                                        ExecRow templateOutputRow,
                                                        TxnView txn,
                                                        boolean staleOnly) throws StandardException, ExecutionException  {

        DataValueDescriptor[] dvds = templateOutputRow.getRowArray();
        dvds[0].setValue(table.getSchemaName());
        Collection<HRegionInfo> regionsToCollect = getCollectedRegions(conn, indexConglomerateId, staleOnly);
        int partitionSize = regionsToCollect.size();
        dvds[1].setValue(indexName);
        dvds[2].setValue(partitionSize);
        dvds[3].setValue(partitionSize);

        long[] statsTableIds = getStatsConglomerateIds();
        Activation activation = conn.getLanguageConnection().getLastActivation();
        List<ExecRow> rows = Lists.newArrayList();
        DataSetProcessor dsp = StreamUtils.sparkDataSetProcessor;
        OperationContext context = dsp.createOperationContext(activation);
        TableScannerBuilder tableScannerBuilder = createIndexStatisticsScanner(table, irg, columnsToCollect, txn);

        SparkDataSet dataSet = (SparkDataSet)dsp.getTableScanner(activation, tableScannerBuilder, (new Long(indexConglomerateId).toString()));

        InsertTableWriterBuilder tableWriterBuilder = getTableWriterBuilder(conn, txn, statsTableIds[1]);

        dataSet.index(new InsertPairFunction()).insertData(tableWriterBuilder, context);
        dvds[4].setValue(dataSet.count());
        rows.add(templateOutputRow.getClone());
        invalidateStatisticsCache(indexConglomerateId);
        return rows;
    }

    private static TableScannerBuilder createIndexStatisticsScanner(TableDescriptor baseTable,
                                                                    IndexRowGenerator irg,
                                                                    List<ColumnDescriptor> columnsToCollect,
                                                                    TxnView txn) throws StandardException, ExecutionException{
        ExecRow indexRow = irg.getIndexRowTemplate();
        int[] baseColumnPositions = irg.baseColumnPositions();
        int keyCols = indexRow.nColumns();
        if (irg.isUnique()) keyCols--;

        int[] fieldLengths = new int[indexRow.nColumns()];
        int[] keyBasePositionMap = new int[indexRow.nColumns()];

        int[] keyTypes = new int[keyCols];
        int[] keyEncodingOrder = new int[keyCols];
        boolean[] keySortOrder = new boolean[keyCols];
        System.arraycopy(irg.isAscending(), 0, keySortOrder, 0, irg.isAscending().length);
        int[] keyDecodingMap = IntArrays.count(keyCols);
        int nextColPos = 1;
        for (int keyColumn : baseColumnPositions) {
            for (ColumnDescriptor cd : columnsToCollect) {
                int colPos = cd.getPosition();
                if (colPos == keyColumn) {
                    /*
                     * We have the column information for this position in the row, so fill
                     * the value
                     */
                    DataTypeDescriptor type = cd.getType();
                    DataValueDescriptor val = type.getNull();
                    indexRow.setColumn(nextColPos, val);
                    keyTypes[nextColPos - 1] = val.getTypeFormatId();
                    keyEncodingOrder[nextColPos - 1] = colPos - 1;
                    fieldLengths[nextColPos - 1] = type.getMaximumWidth();
                    keyBasePositionMap[nextColPos - 1] = colPos;
                    nextColPos++;
                    break;
                }
            }
        }
        HBaseRowLocation value = new HBaseRowLocation();
        indexRow.setColumn(indexRow.nColumns(), value);
        int[] rowDecodingMap;
        if (!irg.isUnique()) {
            keySortOrder[indexRow.nColumns() - 1] = true;
            keyTypes[indexRow.nColumns() - 1] = value.getTypeFormatId();
            rowDecodingMap = new int[0];
        } else {
            keyBasePositionMap[nextColPos - 1] = -1;
            rowDecodingMap = new int[]{indexRow.nColumns() - 1};
        }
        FormatableBitSet collectedKeyColumns = new FormatableBitSet(baseColumnPositions.length);
        for (int i = 0; i < keyEncodingOrder.length; i++) {
            collectedKeyColumns.grow(i + 1);
            collectedKeyColumns.set(i);
        }

        int[] execRowFormatIds = new int[indexRow.nColumns()];
        for (int i = 0; i < indexRow.nColumns(); ++i) {
            execRowFormatIds[i] = indexRow.getColumn(i+1).getFormat().getStoredFormatId();
        }
        Scan scan = createScan(txn);
        return new TableScannerBuilder()
                .transaction(txn)
                .execRowTypeFormatIds(execRowFormatIds)
                .scan(scan)
                .rowDecodingMap(rowDecodingMap)
                .keyColumnEncodingOrder(keyEncodingOrder)
                .keyColumnSortOrder(keySortOrder)
                .keyColumnTypes(keyTypes)
                .keyDecodingMap(keyDecodingMap)
                .accessedKeyColumns(collectedKeyColumns)
                .tableVersion(baseTable.getVersion())
                .fieldLengths(fieldLengths)
                .columnPositionMap(keyBasePositionMap)
                .baseTableConglomId(baseTable.getHeapConglomerateId());
    }

    private static List <ExecRow> collectBaseTableStatistics(TableDescriptor table,
                                                             TxnView txn,
                                                             ExecRow templateOutputRow,
                                                             EmbedConnection conn,
                                                             boolean staleOnly) throws StandardException, ExecutionException {
        List<ExecRow> rows = Lists.newArrayList();
        DataValueDescriptor[] dvds = templateOutputRow.getRowArray();
        dvds[0].setValue(table.getSchemaName());
        long heapConglomerateId = table.getHeapConglomerateId();
        Collection<HRegionInfo> regionsToCollect = getCollectedRegions(conn, heapConglomerateId, staleOnly);
        int partitionSize = regionsToCollect.size();
        dvds[1].setValue(table.getName());
        dvds[2].setValue(partitionSize);
        dvds[3].setValue(partitionSize);

        Activation activation = conn.getLanguageConnection().getLastActivation();
        long[] statsTableIds = getStatsConglomerateIds();
        DataSetProcessor dsp = StreamUtils.sparkDataSetProcessor;
        OperationContext context = dsp.createOperationContext(activation);
        TableScannerBuilder tableScannerBuilder = createTableScannerBuilder(conn, table, txn);
        InsertTableWriterBuilder tableWriterBuilder = getTableWriterBuilder(conn, txn, statsTableIds[1]);
        SparkDataSet dataSet = (SparkDataSet)dsp.getTableScanner(activation, tableScannerBuilder, (new Long(heapConglomerateId).toString()));
        dataSet.index(new InsertPairFunction()).insertData(tableWriterBuilder, context);
        dvds[4].setValue(dataSet.count());
        rows.add(templateOutputRow.getClone());
        invalidateStatisticsCache(heapConglomerateId);
        return rows;
    }

    private static void invalidateStatisticsCache(long conglomId) throws ExecutionException{
        if(!StatisticsStorage.isStoreRunning()) return; //nothing to invalidate
        StatisticsStorage.getPartitionStore().invalidateCachedStatistics(conglomId);
    }

    private static long[] getStatsConglomerateIds() throws ExecutionException {
        try {
            EmbedConnection conn = (EmbedConnection) getDefaultConn();
            LanguageConnectionContext lcc = conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();
            SchemaDescriptor sysSchema = dd.getSystemSchemaDescriptor();

            long[] ids = new long[3];
            TableDescriptor tableColDesc = dd.getTableDescriptor("SYSTABLESTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[0] = tableColDesc.getHeapConglomerateId();
            TableDescriptor colColDesc = dd.getTableDescriptor("SYSCOLUMNSTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[1] = colColDesc.getHeapConglomerateId();
            TableDescriptor physColDesc = dd.getTableDescriptor("SYSPHYSICALSTATS",
                    sysSchema, lcc.getTransactionExecute());
            ids[2] = physColDesc.getHeapConglomerateId();
            return ids;
        } catch (StandardException | SQLException e) {
            throw new ExecutionException(e);
        }
    }
    private static Scan createScan (TxnView txn) {
        Scan scan= SpliceUtils.createScan(txn, false);
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scan.setStartRow(new byte[0]);
        scan.setStopRow(new byte[0]);
        scan.setCacheBlocks(false);
        return scan;
    }

    private static InsertTableWriterBuilder getTableWriterBuilder(EmbedConnection conn,
                                                                  TxnView txn,
                                                                  long columnStatsConglomId) throws StandardException{
        try {
            int[] pkCols = getPkCols(conn, columnStatsConglomId);
            int[] formatIds = getFormatIds(conn, columnStatsConglomId);
            ExecRow execRow = SMSQLUtil.getExecRow(formatIds);


            return new InsertTableWriterBuilder()
                    .heapConglom(columnStatsConglomId)
                    .autoIncrementRowLocationArray(new RowLocation[0])
                    .execRowDefinition(execRow)
                    .execRowTypeFormatIds(formatIds)
                    .spliceSequences(new SpliceSequence[0])
                    .pkCols(pkCols)
                    .tableVersion("2.0")
                    .txn(txn);
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    public static int[] getFormatIds(EmbedConnection conn, long columnStatsConglomId) throws StandardException{
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(columnStatsConglomId);
        int[] formatIds = conglomerate.getFormat_ids();
        return formatIds;
    }

    private static int[] getPkCols(EmbedConnection conn, long columnStatsConglomId) throws StandardException{
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) transactionExecute)
                .findConglomerate(columnStatsConglomId);
        int[] columOrdering = conglomerate.getColumnOrdering();
        int[] pkCols = new int[columOrdering.length];
        for (int i = 0; i < pkCols.length; ++i) {
            pkCols[i] = columOrdering[i] + 1;
        }
        return pkCols;
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
                .accessedKeyColumns(collectedKeyColumns)
                .tableVersion(table.getVersion())
                .fieldLengths(fieldLengths)
                .columnPositionMap(columnPositionMap);
    }

    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn, List<ExecRow> rows) throws
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
        SchemaDescriptor schemaDescriptor = dd.getSchemaDescriptor(schema, lcc.getTransactionExecute(), true);
        if (schemaDescriptor == null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema);
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

    private static Collection<HRegionInfo> getCollectedRegions(Connection conn, long heapConglomerateId, boolean
        staleOnly) throws StandardException {
        //TODO -sf- adjust the regions if staleOnly == true
        return getAllRegions(heapConglomerateId);
    }

    private static Collection<HRegionInfo> getAllRegions(long heapConglomerateId) throws StandardException {
        RegionCache instance = HBaseRegionCache.getInstance();
        byte[] tableName = Long.toString(heapConglomerateId).getBytes();
        instance.invalidate(tableName); //invalidate to force the most up-to-date listing
        try {
            SortedSet<Pair<HRegionInfo, ServerName>> regions = instance.getRegions(tableName);
            SortedSet<HRegionInfo> toCollect = new TreeSet<>();
            for (Pair<HRegionInfo, ServerName> region : regions) {
                //fetch the latest staleness data for that
                toCollect.add(region.getFirst());
            }
            return toCollect;
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        }
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

    private static String getCurrentSchema() throws SQLException {

        EmbedConnection connection = (EmbedConnection) SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = connection.getLanguageConnection();
        String schema = lcc.getCurrentSchemaName();

        return schema;
    }
}
