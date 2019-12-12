/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.FakeColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
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
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
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
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.FluentIterable;
import org.spark_project.guava.collect.Lists;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.splicemachine.derby.utils.EngineUtils.getSchemaDescriptor;
import static com.splicemachine.derby.utils.EngineUtils.verifyTableExists;


/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsAdmin extends BaseAdminProcedures {
    private static final Logger LOG = Logger.getLogger(StatisticsAdmin.class);
    public static final String TABLEID_FROM_SCHEMA = "select tableid from sysvw.systablesView t where t.schemaid = ?";

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

    public static void SET_STATS_EXTRAPOLATION_FOR_COLUMN(String schema,
                                                 String table,
                                                 String columnName,
                                                 short useExtrapolation) throws SQLException {
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
                    byte value = (byte)(useExtrapolation==0 ? 0 : 1);
                    // make sure the column type can support extrapolation
                    if ((value == 1) && !ColumnDescriptor.allowsExtrapolation(descriptor.getType()))
                        throw ErrorState.LANG_STATS_EXTRAPOLATION_NOT_SUPPORTED.newException(columnName, descriptor.getType());
                    descriptor.setUseExtrapolation(value);
                    LanguageConnectionContext languageConnection=conn.getLanguageConnection();
                    TransactionController transactionCompile=languageConnection.getTransactionCompile();
                    transactionCompile.elevate("dictionary");
                    languageConnection.getDataDictionary().setUseExtrapolation(transactionCompile, td.getUUID(), columnName, value);
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName, schema + "." + table);
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void DISABLE_ALL_COLUMN_STATISTICS(String schema,
                                                 String table) throws SQLException {
        schema = EngineUtils.validateSchema(schema);
        table = EngineUtils.validateTable(table);
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn, schema, table);
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            //get the list of index columns whose stats are mandatory
            boolean[] indexColumns = new boolean[columnDescriptorList.size()];

            IndexLister indexLister = td.getIndexLister();
            if (indexLister != null) {
                IndexRowGenerator[] indexRowGenerators = indexLister.getIndexRowGenerators();
                for (IndexRowGenerator irg : indexRowGenerators) {
                    int[] keyColumns = irg.getIndexDescriptor().baseColumnPositions();
                    for (int keyColumn : keyColumns) {
                        indexColumns[keyColumn - 1] = true;
                    }
                }
            }

            // get the list of columns in PK whose stats are also mandatory
            ReferencedKeyConstraintDescriptor keyDescriptor = td.getPrimaryKey();
            if (keyDescriptor != null) {
                int[] pkColumns = keyDescriptor.getReferencedColumns();
                for (int keyColumn : pkColumns) {
                    indexColumns[keyColumn - 1] = true;
                }
            }

            //go through all columns
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                String columnName = descriptor.getColumnName();
                //need to make sure it's not a pk or indexed column
                if (!indexColumns[descriptor.getPosition() - 1]) {
                    descriptor.setCollectStatistics(false);
                    LanguageConnectionContext languageConnection = conn.getLanguageConnection();
                    TransactionController transactionCompile = languageConnection.getTransactionCompile();
                    transactionCompile.elevate("dictionary");
                    languageConnection.getDataDictionary().setCollectStats(transactionCompile, td.getUUID(), columnName, false);
                }
            }
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

    @SuppressWarnings("UnusedDeclaration")
    public static void ENABLE_ALL_COLUMN_STATISTICS(String schema,
                                                String table) throws SQLException {
        schema = EngineUtils.validateSchema(schema);
        table = EngineUtils.validateTable(table);
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn, schema, table);
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                String columnName = descriptor.getColumnName();
                DataTypeDescriptor type = descriptor.getType();
                if (!descriptor.collectStatistics() && ColumnDescriptor.allowsStatistics(type)) {
                    descriptor.setCollectStatistics(true);
                    LanguageConnectionContext languageConnection = conn.getLanguageConnection();
                    TransactionController transactionCompile = languageConnection.getTransactionCompile();
                    transactionCompile.elevate("dictionary");
                    languageConnection.getDataDictionary().setCollectStats(transactionCompile, td.getUUID(), columnName, true);
                }
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static final ResultColumnDescriptor[] COLLECTED_STATS_OUTPUT_COLUMNS = new GenericColumnDescriptor[]{
        new GenericColumnDescriptor("schemaName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("tableName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("partition", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("rowsCollected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("partitionSize", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("partitionCount", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("statsType", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("sampleFraction", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE))
    };

    private static final ResultColumnDescriptor[] COLUMN_STATS_OUTPUT_COLUMNS = new GenericColumnDescriptor[]{
        new GenericColumnDescriptor("schemaName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("tableName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("columnName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("partition", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
        new GenericColumnDescriptor("nullCount", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("totalCount", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("cardinality", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    @SuppressWarnings("unused")
    public static void COLLECT_SCHEMA_STATISTICS(String schema, boolean staleOnly, ResultSet[] outputResults) throws
        SQLException {
        EmbedConnection conn = (EmbedConnection)getDefaultConn();
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
            dropTableStatistics(tds, dd, tc);
            ddlNotification(tc, tds);
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();

            HashMap<Long, Pair<String, String>> display = new HashMap<>();
            ArrayList<StatisticsOperation> statisticsOperations = new ArrayList<>(tds.size());
            for (TableDescriptor td : tds) {
                display.put(td.getHeapConglomerateId(), Pair.newPair(schema, td.getName()));
                statisticsOperations.add(createCollectTableStatisticsOperation(td, false, 0, true, txn, conn));
            }

            IteratorNoPutResultSet resultsToWrap = wrapResults(conn,
                    displayTableStatistics(statisticsOperations,true, dd, transactionExecute, display), COLLECTED_STATS_OUTPUT_COLUMNS);
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
        doStatsCollectionForTables(schema, table, false, 0.0, staleOnly, true, outputResults);
    }

    public static void COLLECT_TABLE_SAMPLE_STATISTICS(String schema,
                                                       String table,
                                                       double sample,
                                                       boolean staleOnly,
                                                       ResultSet[] outputResults) throws SQLException {
        doStatsCollectionForTables(schema, table, true, sample, staleOnly, true, outputResults);
    }

    @SuppressWarnings({"unchecked"})
    public static void COLLECT_NONMERGED_TABLE_STATISTICS(String schema,
                                                String table,
                                                boolean staleOnly,
                                                ResultSet[] outputResults) throws SQLException {
        doStatsCollectionForTables(schema, table, false, 0.0, staleOnly, false, outputResults);
    }

    public static void COLLECT_NONMERGED_TABLE_SAMPLE_STATISTICS(String schema,
                                                       String table,
                                                       double sample,
                                                       boolean staleOnly,
                                                       ResultSet[] outputResults) throws SQLException {
        doStatsCollectionForTables(schema, table, true, sample, staleOnly, false, outputResults);
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

    public static void FAKE_TABLE_STATISTICS(String schema,
                                             String table,
                                             long rowCount,
                                             int meanRowWidth,
                                             long numPartitions,
                                             ResultSet[] outputResults) throws SQLException {

        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            schema = EngineUtils.validateSchema(schema);
            table = EngineUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);

            if (rowCount < 0)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("table", "row count cannot be a negative value");

            if (meanRowWidth <= 0)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("table", "meanRowWidth has to be greater than 0");
            if (numPartitions <= 0)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("table", "number of partitions has to be greater than 0");

            List<TableDescriptor> tds = Collections.singletonList(tableDesc);
            authorize(tds);
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            dd.startWriting(conn.getLanguageConnection());
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            dropTableStatistics(tds,dd,tc);
            ddlNotification(tc, tds);

            // compose the fake table stats row
            ExecRow statsRow;
            int statsType = SYSTABLESTATISTICSRowFactory.FAKE_MERGED_STATS;
            long conglomerateId = tableDesc.getHeapConglomerateId();

            statsRow = StatisticsAdmin.generateRowFromStats(conglomerateId, "-All-", rowCount, rowCount*meanRowWidth, meanRowWidth, numPartitions, statsType, 0.0d);
            dd.addTableStatistics(statsRow, tc);
            ExecRow resultRow = generateOutputRow(schema, table, statsRow);

            IteratorNoPutResultSet resultsToWrap = wrapResults(
                    conn,
                    Lists.newArrayList(resultRow), COLLECTED_STATS_OUTPUT_COLUMNS);
            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    public static void FAKE_COLUMN_STATISTICS(String schema,
                                              String table,
                                              String column,
                                              double nullCountRatio,
                                              long rpv,
                                              ResultSet[] outputResults) throws SQLException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            schema = EngineUtils.validateSchema(schema);
            table = EngineUtils.validateTable(table);
            column = EngineUtils.validateColumnName(column);
            TableDescriptor td = verifyTableExists(conn, schema, table);
            //verify that that column exists
            int columnId = -1;
            ColumnDescriptor columnDescriptor = null;
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for (ColumnDescriptor descriptor : columnDescriptorList) {
                if (descriptor.getColumnName().equalsIgnoreCase(column)) {
                    columnId = descriptor.getPosition();
                    columnDescriptor = descriptor;
                    break;
                }
            }
            if (columnId == -1)
                throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(column, schema + "." + table);
            List<TableDescriptor> tds = Collections.singletonList(td);
            authorize(tds);
            DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
            dd.startWriting(conn.getLanguageConnection());
            TransactionController tc = conn.getLanguageConnection().getTransactionExecute();
            tc.elevate("statistics");
            // get the row count from table stats
            long totalCount = getRowCountFromTableStats(td.getHeapConglomerateId(), dd, tc);

            if (totalCount < 0)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("column", "table stats do not exist, please add table stats first");

            if (nullCountRatio < 0 || nullCountRatio > 1)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("column", "null count ratio should be in the range of [0,1]");

            long nullCount = (long)(nullCountRatio * totalCount);

            if (rpv > totalCount - nullCount || rpv < 1)
                throw ErrorState.LANG_INVALID_FAKE_STATS.newException("column", "rows per value shouldn't be less than 1 or larger than the total number of not-null count : " + (totalCount - nullCount));

            long cardinality = (long)(((double)(totalCount- nullCount))/rpv);

            dropColumnStatistics(td.getHeapConglomerateId(), columnId, dd,tc);

            ddlNotification(tc, tds);
            // compose the fake column stats row
            long conglomerateId = td.getHeapConglomerateId();

            FakeColumnStatisticsImpl columnStatistics = new FakeColumnStatisticsImpl(columnDescriptor.getType().getNull(), nullCount, totalCount, cardinality);
            // compose the entry for a given column
            ExecRow statsRow = StatisticsAdmin.generateRowFromStats(conglomerateId, "-All-", columnId, columnStatistics);
            dd.addColumnStatistics(statsRow, tc);

            ExecRow resultRow = generateOutputRowForColumnStats(schema, table, column, "-All-", nullCount, totalCount, cardinality);

            IteratorNoPutResultSet resultsToWrap = wrapResults(
                    conn,
                    Lists.newArrayList(resultRow), COLUMN_STATS_OUTPUT_COLUMNS);
            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }

    }

    private static void ddlNotification(TransactionController tc,  List<TableDescriptor> tds) throws StandardException {
        DDLChange ddlChange = ProtoUtil.alterStats(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),tds);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static void doStatsCollectionForTables(String schema,
                                                   String table,
                                                   boolean useSample,
                                                   double samplePercent,
                                                   boolean staleOnly,
                                                   boolean mergeStats,
                                                   ResultSet[] outputResults) throws SQLException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        try {
            schema = EngineUtils.validateSchema(schema);
            table = EngineUtils.validateTable(table);
            TableDescriptor tableDesc = verifyTableExists(conn, schema, table);
            List<TableDescriptor> tds = Collections.singletonList(tableDesc);
            authorize(tds);
            //check if sample fraction is in the valid range
            if (useSample) {
                if (samplePercent<0.0 || samplePercent>100.0)
                    throw ErrorState.LANG_INVALID_VALUE_RANGE.newException("samplePercent value " + samplePercent, "[0,100]");
            }
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
                            createCollectTableStatisticsOperation(tableDesc, useSample, samplePercent/100, mergeStats, txn, conn)
                            ),
                            mergeStats,
                            dd, tc, display), COLLECTED_STATS_OUTPUT_COLUMNS);
            outputResults[0] = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } catch (ExecutionException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        }
    }

    private static StatisticsOperation createCollectTableStatisticsOperation(TableDescriptor table,
                                                                             boolean useSample,
                                                                             double sampleFraction,
                                                                             boolean mergeStats,
                                                                             TxnView txn,
                                                                             EmbedConnection conn) throws StandardException, ExecutionException {
        long heapConglomerateId = table.getHeapConglomerateId();
        Activation activation = conn.getLanguageConnection().getLastActivation();
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();

        ScanSetBuilder ssb = dsp.newScanSet(null,Long.toString(heapConglomerateId));
        ssb.tableVersion(table.getVersion());
        ScanSetBuilder scanSetBuilder = createTableScanner(ssb,conn,table,txn);
        String scope = getScopeName(table);
        // no sample stats support on mem platform
        if (dsp.getType() != DataSetProcessor.Type.SPARK) {
            useSample = false;
            sampleFraction = 0.0d;
        }
        List<ColumnDescriptor> colsToCollect = getCollectedColumns(conn, table);
        DataTypeDescriptor[] dtds = new DataTypeDescriptor[colsToCollect.size()];
        int index = 0;
        for (ColumnDescriptor descriptor : colsToCollect ) {
            dtds[index++] = descriptor.getType();
        }
        StatisticsOperation op = new StatisticsOperation(scanSetBuilder,useSample,sampleFraction,mergeStats,scope,activation,dtds);
        return op;
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

        List<ColumnDescriptor> colsToCollect = getCollectedColumns(conn, table);
        ExecRow row = new ValueRow(colsToCollect.size());
        BitSet accessedColumns = new BitSet(table.getMaxStorageColumnID());
        int outputCol = 0;
        int[] columnPositionMap = new int[table.getNumberOfColumns()];
        Arrays.fill(columnPositionMap, -1);
        int[] allColumnLengths = new int[table.getMaxStorageColumnID()];
        for (ColumnDescriptor descriptor : colsToCollect) {
            accessedColumns.set(descriptor.getStoragePosition() - 1);
            row.setColumn(outputCol + 1, descriptor.getType().getNull());
            columnPositionMap[outputCol] = descriptor.getPosition();
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
        return builder.transaction(txn)
                .metricFactory(Metrics.basicMetricFactory())
                .template(row)
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
                .partitionByColumns(table.getPartitionBy())
                ;
    }

    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn, Iterable<ExecRow> rows, ResultColumnDescriptor[] columnDescriptors) throws
        StandardException {
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnDescriptors,
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

    public static List<TableDescriptor> getAllTableDescriptors(SchemaDescriptor sd, EmbedConnection conn) throws
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

                    if (tableDescriptor != null && !tableDescriptor.getConglomerateDescriptorList().isEmpty()) {
                        tds.add(tableDescriptor);
                    }
                }
                return tds;
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static final Comparator<ColumnDescriptor> order = new Comparator<ColumnDescriptor>() {
        @Override
        public int compare(ColumnDescriptor o1, ColumnDescriptor o2) {
            return o1.getPosition() - o2.getPosition();
        }
    };

    private static List<ColumnDescriptor> getCollectedColumns(EmbedConnection conn, TableDescriptor td) throws StandardException {
        ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
        List<ColumnDescriptor> toCollect = new ArrayList<>(columnDescriptorList.size());

        /* check the default collect stats behavior, whether to collect stats on all columns or just index columns */
        String collectStatsMode = PropertyUtil.getServiceProperty(conn.getLanguageConnection().getTransactionCompile(),
                Property.COLLECT_INDEX_STATS_ONLY);
        boolean collectIndexStatsOnly = Boolean.valueOf(collectStatsMode);

        boolean[] indexColumns = new boolean[columnDescriptorList.size()];

        IndexLister indexLister = td.getIndexLister();
        if (collectIndexStatsOnly) {
            // get all other index columns
            if (indexLister != null) {
                IndexRowGenerator[] indexRowGenerators = indexLister.getIndexRowGenerators();
                for (IndexRowGenerator irg : indexRowGenerators) {
                    int[] keyColumns = irg.getIndexDescriptor().baseColumnPositions();
                    for (int keyColumn : keyColumns) {
                        indexColumns[keyColumn - 1] = true;
                    }
                }
            }
        }


        /*
         * Get all the enabled statistics columns
         */
        for (ColumnDescriptor columnDescriptor : columnDescriptorList) {
            if (!collectIndexStatsOnly || indexColumns[columnDescriptor.getPosition()-1]) {
                if (columnDescriptor.collectStatistics())
                    toCollect.add(columnDescriptor);
            }
        }
        /*
         * Add in any disabled key columns.
         *
         * We want to collect for all key columns always, because they are very important when
         * comparing index columns. By default, we turn them on when possible, but even if they are disabled
         * for some reason, we should still collect them. Of course, we should also not be able to disable
         * keyed columns, but that's a to-do for now.
         */
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

        // we should always include primary key if it exists
        ReferencedKeyConstraintDescriptor keyDescriptor = td.getPrimaryKey();
        if (keyDescriptor != null) {
            int[] pkColumns = keyDescriptor.getReferencedColumns();
            for (int keyColumn : pkColumns) {
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

    public static ExecRow generateRowFromStats(long conglomId,
                                               String partitionId,
                                               long rowCount,
                                               long partitionSize,
                                               int meanRowWidth,
                                               long numberOfPartitions,
                                               int statsType,
                                               double sampleFraction) throws StandardException {
        ExecRow row = new ValueRow(SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.TIMESTAMP,new SQLTimestamp(new Timestamp(System.currentTimeMillis())));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STALENESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.INPROGRESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH,new SQLInteger(meanRowWidth));
        row.setColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS,new SQLLongint(numberOfPartitions));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE,new SQLInteger(statsType));
        row.setColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION, new SQLDouble(sampleFraction));
        return row;
    }

    public static ExecRow generateRowFromStats(long conglomId,
                                               String partitionId,
                                               long timestamp,
                                               boolean isStale,
                                               boolean inProgress,
                                               long rowCount,
                                               long partitionSize,
                                               int meanRowWidth,
                                               long numberOfPartitions,
                                               int statsType,
                                               double sampleFraction) throws StandardException {
        ExecRow row = new ValueRow(SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.TIMESTAMP,new SQLTimestamp(new Timestamp(timestamp)));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STALENESS,new SQLBoolean(isStale));
        row.setColumn(SYSTABLESTATISTICSRowFactory.INPROGRESS,new SQLBoolean(inProgress));
        row.setColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH,new SQLInteger(meanRowWidth));
        row.setColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS,new SQLLongint(numberOfPartitions));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE,new SQLInteger(statsType));
        row.setColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION, new SQLDouble(sampleFraction));
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

    public static ExecRow generateOutputRowForColumnStats(String schemaName,
                                                          String tableName,
                                                          String columnName,
                                                          String partitionName,
                                                          long nullCount,
                                                          long totalCount,
                                                          long cardinality) throws StandardException {
        ExecRow row = new ValueRow(7);
        row.setColumn(1,new SQLVarchar(schemaName));
        row.setColumn(2,new SQLVarchar(tableName));
        row.setColumn(3,new SQLVarchar(columnName));
        row.setColumn(4,new SQLVarchar(partitionName));
        row.setColumn(5,new SQLLongint(nullCount));
        row.setColumn(6,new SQLLongint(totalCount));
        row.setColumn(7,new SQLLongint(cardinality));
        return row;
    }

    public static ExecRow generateOutputRow(String schemaName, String tableName, ExecRow partitionRow) throws StandardException {
        ExecRow row = new ValueRow(8);
        row.setColumn(1,new SQLVarchar(schemaName));
        row.setColumn(2,new SQLVarchar(tableName));
        row.setColumn(3,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID));
        row.setColumn(4,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT));
        row.setColumn(5,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE));
        row.setColumn(6,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS));
        row.setColumn(7,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE));
        row.setColumn(8,partitionRow.getColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION));
        return row;
    }


    public static Iterable displayTableStatistics(ArrayList<StatisticsOperation> collectOps,
                                                  boolean mergeStats,
                                                  final DataDictionary dataDictionary,
                                                  final TransactionController tc,
                                                  final HashMap<Long, Pair<String, String>> displayPair) throws StandardException {
        // Schedule the first <maximumConcurrent> jobs
        int maximumConcurrent = EngineDriver.driver().getConfiguration().getCollectSchemaStatisticsMaximumConcurrent();
        for (int i = 0; i < maximumConcurrent && i < collectOps.size(); ++i) {
            collectOps.get(i).openCore();
        }
        // Handle the next jobs as we go: (One job returns -> one job can start), ensuring <maximumConcurrent> jobs at all time
        Iterable<StatisticsOperation> movingExecutionWindow = () -> new Iterator<StatisticsOperation>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i < collectOps.size();
            }

            @Override
            public StatisticsOperation next() {
                if ((long)i + (long)maximumConcurrent < (long)collectOps.size()) {
                    try {
                        collectOps.get(i + maximumConcurrent).openCore();
                    } catch (StandardException e) {
                        throw new RuntimeException(e);
                    }
                }
                return collectOps.get(i++);
            }
        };
        if (mergeStats) {
            return FluentIterable.from(movingExecutionWindow).transformAndConcat(new Function<StatisticsOperation, Iterable<ExecRow>>() {
                @Nullable
                @Override
                public Iterable<ExecRow> apply(@Nullable StatisticsOperation input) {
                    try {
                        // We have to create a new savepoint because we already returned from the opening of the result set
                        // and derby released the prior savepoint for us. If we don't create one we'd end up inserting the
                        // rows with the user transaction, and that's problematic especially if we had to remove existing
                        // statistics, since those deletes would mask these new inserts.
                        tc.setSavePoint("statistics", null);
                        tc.elevate("statistics");
                        final Iterator iterator = new Iterator<ExecRow>() {
                            private ExecRow nextRow;
                            private boolean fetched = false;
                            // data structures to accumulate the partition stats
                            private long conglomId = 0;
                            private long rowCount = 0L;
                            private long totalSize = 0;
                            private int avgRowWidth = 0;
                            private long numberOfPartitions = 0;
                            private int statsType = SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS;
                            private double sampleFraction = 0.0d;

                            @Override
                            public boolean hasNext() {
                                try {
                                    if (!fetched) {
                                        nextRow = input.getNextRowCore();
                                        while (nextRow != null) {
                                            fetched = true;
                                            if (nextRow.nColumns() == 2) {
                                                int columnId = nextRow.getColumn(1).getInt();
                                                ByteArrayInputStream bais = new ByteArrayInputStream(nextRow.getColumn(2).getBytes());
                                                ObjectInputStream ois = new ObjectInputStream(bais);
                                                // compose the entry for a given column
                                                ExecRow statsRow = StatisticsAdmin.generateRowFromStats(conglomId, "-All-", columnId, (ColumnStatisticsImpl) ois.readObject());
                                                dataDictionary.addColumnStatistics(statsRow, tc);
                                                bais.close();
                                            } else {
                                                // process tablestats row
                                                conglomId = nextRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.CONGLOMID).getLong();
                                                long partitionRowCount = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong();
                                                rowCount = partitionRowCount;
                                                totalSize = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE).getLong();
                                                avgRowWidth = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getInt();
                                                numberOfPartitions = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS).getLong();
                                                statsType = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE).getInt();
                                                sampleFraction = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION).getDouble();
                                            }
                                            nextRow = input.getNextRowCore();
                                        }
                                    }
                                    if (!fetched)
                                        tc.releaseSavePoint("statistics", null);
                                    return fetched;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public ExecRow next() {
                                try {
                                    fetched = false;
                                    // insert rows to dictionary tables, and return
                                    ExecRow statsRow;
                                    //change statsType to 2: merged full stats or 3: merged sample stats
                                    if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS)
                                        statsType = SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS;
                                    else if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_NONMERGED_STATS)
                                        statsType = SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS;
                                    statsRow = StatisticsAdmin.generateRowFromStats(conglomId, "-All-", rowCount, totalSize, avgRowWidth, numberOfPartitions, statsType, sampleFraction);
                                    dataDictionary.addTableStatistics(statsRow, tc);
                                    Pair<String, String> pair = displayPair.get(conglomId);
                                    return generateOutputRow(pair.getFirst(), pair.getSecond(), statsRow);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        return () -> iterator;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } else {
            return FluentIterable.from(movingExecutionWindow).transformAndConcat(new Function<StatisticsOperation, Iterable<ExecRow>>() {
                @Nullable
                @Override
                public Iterable<ExecRow> apply(@Nullable StatisticsOperation input) {
                    try {
                        final Iterator iterator = new Iterator<ExecRow>() {
                            private ExecRow nextRow;
                            private boolean fetched = false;
                            @Override
                            public boolean hasNext() {
                                try {
                                    if (!fetched) {
                                        nextRow = input.getNextRowCore();
                                        while (nextRow != null && nextRow.nColumns() == SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT) {
                                            dataDictionary.addColumnStatistics(nextRow, tc);
                                            nextRow = input.getNextRowCore();
                                        }
                                        fetched = true;
                                    }
                                    return nextRow != null;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public ExecRow next() {
                                try {
                                    fetched = false;
                                    dataDictionary.addTableStatistics(nextRow, tc);
                                    Pair<String,String> pair = displayPair.get(nextRow.getColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID).getLong());
                                    return generateOutputRow(pair.getFirst(),pair.getSecond(),nextRow);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        };
                        return () -> iterator;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
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

    private static long getRowCountFromTableStats(long conglomerateId,
                                                  DataDictionary dd,
                                                  TransactionController tc) throws StandardException {
        long totalCount = 0;
        List<PartitionStatisticsDescriptor> partitionStatsDescriptors = dd.getPartitionStatistics(conglomerateId, tc);

        if (partitionStatsDescriptors.isEmpty())
           return -1;

        double sampleFraction = 0.0d;
        int statsType = partitionStatsDescriptors.get(0).getStatsType();
        boolean isSampleStats = statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_NONMERGED_STATS || statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS;
        if (isSampleStats)
            sampleFraction = partitionStatsDescriptors.get(0).getSampleFraction();

        for (PartitionStatisticsDescriptor item: partitionStatsDescriptors) {
            totalCount += item.getRowCount();
        }

        if (isSampleStats)
            totalCount = (long)((double)totalCount/sampleFraction);

        return totalCount;
    }

    private static void dropColumnStatistics(long conglomerateId,
                                             int columnId,
                                             DataDictionary dd,
                                             TransactionController tc) throws StandardException {
        dd.deleteColumnStatisticsByColumnId(conglomerateId, columnId, tc);

    }
}
