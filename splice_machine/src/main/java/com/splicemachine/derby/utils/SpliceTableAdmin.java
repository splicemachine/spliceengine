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

package com.splicemachine.derby.utils;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.util.NetworkUtils;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.ddl.DDLMessage.TentativeIndex;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.storage.CheckTableResult;
import com.splicemachine.derby.impl.storage.CheckTableUtils;
import com.splicemachine.derby.impl.storage.DistributedCheckTableJob;
import com.splicemachine.derby.impl.storage.SpliceRegionAdmin;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionLoad;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by jyuan on 5/22/18.
 */
public class SpliceTableAdmin {

    private static final Logger LOG = Logger.getLogger(SpliceTableAdmin.class);
    private static int MB = 1024*1024;

    public enum Level{
        FAST (1),
        DETAIL (2)
        ;
        private final int levelCode;

        private Level(int levelCode) {
            this.levelCode = levelCode;
        }
    }

    public static void FIX_TABLE(String schemaName, String tableName, String indexName,
                                   String outputFile, final ResultSet[] resultSet) throws Exception {
        CHECK_TABLE(schemaName, tableName, indexName, Level.DETAIL.levelCode, outputFile, true, resultSet);
    }

    public static void CHECK_TABLE(String schemaName, String tableName, String indexName, int level,
                                   String outputFile, final ResultSet[] resultSet) throws Exception {
        CHECK_TABLE(schemaName, tableName, indexName, level, outputFile, false, resultSet);
    }
    public static void CHECK_TABLE(String schemaName, String tableName, String indexName, int level,
                                   String outputFile, boolean fix, final ResultSet[] resultSet) throws Exception {

        if (outputFile == null || outputFile.trim().length() == 0) {
            throw StandardException.newException(SQLState.INVALID_PARAMETER, "outputFile", outputFile==null?"null":outputFile);
        }

        outputFile = outputFile.trim();
        if (tableName == null && indexName == null) {
            CHECK_SCHEMA(schemaName, level, outputFile, fix, resultSet);
        }
        else if (tableName != null && indexName == null) {
            CHECK_TABLE(schemaName, tableName, level, outputFile, fix, resultSet);
        }
        else if (tableName != null && indexName != null){
            CHECK_INDEX(schemaName, tableName, indexName, level, outputFile, fix, resultSet);
        }
        else {
            throw StandardException.newException(SQLState.INVALID_PARAMETER, "indexName", indexName);
        }
    }

    /**
     * This system procedure checks consistency of all tables in a schema
     * @param schemaName
     * @param level
     * @param outputFile
     * @param resultSet
     * @throws Exception
     */
    public static void CHECK_SCHEMA(String schemaName,
                                    int level,
                                    String outputFile,
                                    boolean fix,
                                    final ResultSet[] resultSet) throws Exception {
        FSDataOutputStream out = null;
        FileSystem fs = null;
        Map<String, List<String>> errors = new HashMap<>();
        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            Activation activation = lcc.getLastActivation();
            Configuration conf = (Configuration) SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate();
            String schema = EngineUtils.validateSchema(schemaName);
            fs = FileSystem.get(URI.create(outputFile), conf);

            out = fs.create(new Path(outputFile));
            // check each table in the schema
            List<String> tables = getTables(schema);
            for (String table : tables) {
                Map<String, List<String>> result = checkTable(schema, table, null, level, fix);
                if (result != null) {
                    for (Map.Entry<String, List<String>> entry : result.entrySet()) {
                        String key = entry.getKey();
                        List val = entry.getValue();
                        errors.put(key, val);
                    }
                }
            }
            resultSet[0] = processResults(errors, out, activation, outputFile);
        } finally {
            if (out != null) {
                out.close();
                if (errors == null || errors.size() == 0) {
                    fs.delete(new Path(outputFile), true);
                }
            }

        }
    }

    /**
     * This system procedure checks table consistency
     * @param schemaName
     * @param tableName
     * @param level
     * @param outputFile
     * @param resultSet
     * @throws Exception
     */
    public static void CHECK_TABLE(String schemaName, String tableName, int level,
                                   String outputFile, boolean fix, final ResultSet[] resultSet) throws Exception {

        FSDataOutputStream out = null;
        FileSystem fs = null;
        Map<String, List<String>> errors = null;
        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            Activation activation = lcc.getLastActivation();

            Configuration conf = (Configuration) SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate();
            String schema = EngineUtils.validateSchema(schemaName);
            String table = EngineUtils.validateTable(tableName);
            fs = FileSystem.get(URI.create(outputFile), conf);
            out = fs.create(new Path(outputFile));


            errors = checkTable(schema, table, null, level, fix);
            resultSet[0] = processResults(errors, out, activation, outputFile);
        } finally {
            if (out != null) {
                out.close();
                if (errors == null || errors.size() == 0) {
                    fs.delete(new Path(outputFile), true);
                }
            }

        }
    }


    public static void CHECK_INDEX(String schemaName, String tableName, String indexName, int level,
                                   String outputFile, boolean fix, final ResultSet[] resultSet) throws Exception {
        FSDataOutputStream out = null;
        FileSystem fs = null;
        Map<String, List<String>> errors = null;

        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            Activation activation = lcc.getLastActivation();

            Configuration conf = (Configuration) SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate();
            String schema = EngineUtils.validateSchema(schemaName);
            String table = EngineUtils.validateTable(tableName);
            String index = EngineUtils.validateTable(indexName);

            fs = FileSystem.get(URI.create(outputFile), conf);
            out = fs.create(new Path(outputFile));

            errors = checkTable(schema, table, index, level, fix);
            resultSet[0] = processResults(errors, out, activation, outputFile);

        } finally {
            if (out != null) {
                out.close();
                if (errors == null || errors.size() == 0) {
                    fs.delete(new Path(outputFile), true);
                }
            }

        }

    }

    public static String getIndexName(ConglomerateDescriptorList cds, long conglomerateNumber) {
        for (int i = 0; i < cds.size(); ++i) {
            ConglomerateDescriptor cd = cds.get(i);
            if (cd.getConglomerateNumber() == conglomerateNumber) {
                return cd.getObjectName();
            }
        }
        return null;
    }

    private static Map<String, List<String>> checkTable(String schema,
                                                        String table,
                                                        String index,
                                                        int level,
                                                        boolean fix) throws Exception {

        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc = lcc.getTransactionExecute();
        TxnView txn = ((SpliceTransactionManager) tc).getActiveStateTxn();
        Activation activation = lcc.getLastActivation();

        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schema, tc, true);
        if (sd == null) {
            throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schema);
        }
        TableDescriptor td = dd.getTableDescriptor(table, sd, tc);
        if (td == null) {
            throw StandardException.newException(SQLState.TABLE_NOT_FOUND, table);
        }

        if (index != null) {
            ConglomerateDescriptor indexCD = SpliceRegionAdmin.getIndex(td, index);
            if (indexCD == null) {
                throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, index);
            }
        }
        ConglomerateDescriptorList cdList = td.getConglomerateDescriptorList();
        List<TentativeIndex> tentativeIndexList = new ArrayList();

        for (ConglomerateDescriptor searchCD : cdList) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                if (index == null && !isEligibleConstraint(cdList, searchCD)) {
                    continue;
                }
                if (index == null || searchCD.getObjectName().compareToIgnoreCase(index) == 0) {
                    DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                            activation.getLanguageConnectionContext(),
                            td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                            td, searchCD.getIndexDescriptor(), td.getDefaultValue(searchCD.getIndexDescriptor().baseColumnPositions()[0]));
                    tentativeIndexList.add(ddlChange.getTentativeIndex());
                }
            }
        }

        Map<String, List<String>> errors = null;
        if (tentativeIndexList.size() > 0) {
            errors = checkIndexes(td, schema, table, cdList, tentativeIndexList, level, fix);
        }
        return errors;
    }

    /**
     * If a constraint is reusing an index, exclude it from index list. No need to check the index more than once
     * @param cdList
     * @param cd
     * @return
     */
    private static boolean isEligibleConstraint(ConglomerateDescriptorList cdList, ConglomerateDescriptor cd) {
        // return false if it is not an index or constraint
        if (!cd.isIndex() && !cd.isConstraint())
            return false;

        for (ConglomerateDescriptor conglomerateDescriptor : cdList) {
            if (cd == conglomerateDescriptor)
                continue;

            // return false if the constraint is reusing an index
            if (!conglomerateDescriptor.isConstraint() &&
                    conglomerateDescriptor.isIndex() &&
                    conglomerateDescriptor.getConglomerateNumber() == cd.getConglomerateNumber()) {
                return false;
            }
        }
        return true;
    }
    private static void printErrorMessages(FSDataOutputStream out, Map<String, List<String>> errors) throws IOException {

        for(Map.Entry<String, List<String>> entry : errors.entrySet()) {
            String index = entry.getKey();
            List<String> messages = entry.getValue();

            out.writeBytes(index + ":\n");
            for (String message : messages) {
                out.writeBytes("\t" + message + "\n");
            }
        }
    }

    private static Map<String, List<String>> checkIndexes(TableDescriptor td,
                                                          String schema,
                                                          String table,
                                                          ConglomerateDescriptorList cdList,
                                                          List<TentativeIndex> tentativeIndexList,
                                                          int level,
                                                          boolean fix) throws Exception {
        if (level == Level.FAST.levelCode ) {
            return checkIndexesFast(schema, table, cdList, tentativeIndexList);
        }
        else if (level == Level.DETAIL.levelCode){
            return checkIndexesInDetail(td, schema, table, tentativeIndexList, fix);
        }
        else {
            throw StandardException.newException(SQLState.INVALID_CONSISTENCY_LEVEL);
        }
    }

    private static Map<String, List<String>> checkIndexesFast(String schema, String table,
                                                              ConglomerateDescriptorList cdList,
                                                              List<TentativeIndex> tentativeIndexList) throws IOException, SQLException {
        boolean consistent = true;
        Map<String, List<String>> errors = new HashMap<>();
        long tableCount = countTable(schema, table, null);
        List<String> message = Lists.newArrayList();
        message.add("count = " + tableCount);
        errors.put(table, message);
        for (TentativeIndex tentativeIndex:tentativeIndexList) {
            String indexName = getIndexName(cdList, tentativeIndex.getIndex().getConglomerate());
            DDLMessage.Index index = tentativeIndex.getIndex();
            message = Lists.newArrayList();
            if (index.getExcludeDefaults() || index.getExcludeNulls()) {
                consistent = false;
                message.add("Index count and base table count are not expected to match because index excludes null or " +
                        "default keys. The index should be checked at level 2");
            }
            else {
                long indexCount = countTable(schema, table, indexName);
                if (indexCount != tableCount) {
                    consistent = false;
                }
                message.add("count = " + indexCount);
            }
            errors.put(indexName, message);
        }
        if (consistent){
            errors.clear();
        }
        return errors;
    }


    private static long countTable(String schema, String table, String index) throws SQLException {
        String sql = String.format(
                "select count(*) from %s.%s --splice-properties index=%s", schema, table, index==null?"null":index);
        Connection connection = SpliceAdmin.getDefaultConn();
        try(PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            long count = 0;
            if (rs.next()) {
                count = rs.getLong(1);
            }
            return count;
        }
    }

    private static Map<String, List<String>> checkIndexesInDetail(TableDescriptor td, String schema, String table,
                                                                  List<TentativeIndex> tentativeIndexList,
                                                                  boolean fix) throws Exception {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc = lcc.getTransactionExecute();
        tc.elevate("check_table");
        TxnView txn = ((SpliceTransactionManager) tc).getActiveStateTxn();
        Activation activation = lcc.getLastActivation();
        Boolean useSpark = (Boolean)lcc.getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.USEOLAP);
        String conglomId = Long.toString(tentativeIndexList.get(0).getTable().getConglomerate());
        Collection<PartitionLoad> partitionLoadCollection = EngineDriver.driver().partitionLoadWatcher().tableLoad(conglomId, true);

        boolean distributed = false;
        if (useSpark == null) {
            for (PartitionLoad load : partitionLoadCollection) {
                if (load.getMemStoreSize() > 1 * MB || load.getStorefileSize() > 1 * MB)
                    distributed = true;
            }
        } else {
            distributed = useSpark;
        }
        boolean isSystemTable = schema.equalsIgnoreCase("SYS");
        SIDriver driver = SIDriver.driver();
        String hostname = NetworkUtils.getHostname(driver.getConfiguration());
        SConfiguration config = SIDriver.driver().getConfiguration();
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        int localPort = config.getNetworkBindPort();
        int sessionId = activation.getLanguageConnectionContext().getInstanceNumber();
        String session = hostname + ":" + localPort + "," + sessionId;
        String jobGroup = userId + " <" + session + "," + txn.getTxnId() + ">";

        Map<String, List<String>> errors = null;
        if (distributed) {
            OlapClient olapClient = EngineDriver.driver().getOlapClient();
            ActivationHolder ah = new ActivationHolder(activation, null);
            DistributedCheckTableJob checkTableJob = new DistributedCheckTableJob(ah, txn, schema, table,
                    tentativeIndexList, fix, jobGroup, isSystemTable);
            Future<CheckTableResult> futureResult = olapClient.submit(checkTableJob);
            CheckTableResult result = null;
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
            errors = result.getResults();
        }
        else  {
            errors = CheckTableUtils.checkTable(schema, table, td, tentativeIndexList, Long.parseLong(conglomId),
                    false, fix, isSystemTable, txn, activation, jobGroup);
        }
        return errors;
    }

    private static List<String> getTables(String schemaName) throws SQLException {
        String sql = "select tablename from sys.systables t, sys.sysschemas s where s.schemaname=?" +
                " and s.schemaid=t.schemaid";
        Connection connection = SpliceAdmin.getDefaultConn();
        List<String> tables = Lists.newArrayList();
        try(PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try(ResultSet rs = ps.executeQuery()) {
                while(rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            return tables;
        }
    }

    private static ResultSet processResults(Map<String, List<String>> errors,
                                              FSDataOutputStream out,
                                              Activation activation,
                                            String outputFile) throws IOException, SQLException {
        String message = null;
        if (errors == null || errors.size() == 0) {
            message = String.format("No inconsistencies were found.");
        } else {
            printErrorMessages(out, errors);
            message = String.format("Found inconsistencies. Check %s for details.", outputFile);
        }

        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        List<ExecRow> rows = new ArrayList<>(1);
        ExecRow row = new ValueRow(1);
        row.setColumn(1, new SQLVarchar(message));
        rows.add(row);

        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, new GenericColumnDescriptor[]{
                new GenericColumnDescriptor("RESULT", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 120))},
                activation);
        try {
            resultsToWrap.openCore();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
        return new EmbedResultSet40(conn, resultsToWrap, false, null, true);
    }
}
