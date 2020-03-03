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

package com.splicemachine.replication;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.ReplicationPeerDescription;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ReplicationDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.catalog.SYSREPLICATIONRowFactory;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.impl.storage.SpliceRegionAdmin;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.procedures.ProcedureUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by jyuan on 2/8/19.
 */
public class ReplicationSystemProcedure {

    private static Logger LOG = Logger.getLogger(ReplicationSystemProcedure.class);

    public static void REPLICATION_ENABLED(String schemaName, String tableName, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            schemaName = EngineUtils.validateSchema(schemaName);
            tableName = EngineUtils.validateTable(tableName);
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
            long conglomerateId = td.getHeapConglomerateId();
            PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
            boolean enabled = admin.replicationEnabled(Long.toString(conglomerateId));
            resultSets[0] = ProcedureUtils.generateResult("Enabled", enabled?"TRUE":"FALSE");
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DUMP_UNREPLICATED_WALS(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            String result = replicationManager.dumpUnreplicatedWals();
            resultSets[0] = ProcedureUtils.generateResult("Success", result);
        }
        catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void GET_REPLICATED_WAL_POSITION(String wal, ResultSet[] resultSets) throws StandardException, SQLException {
        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
        String walPosition = replicationManager.getReplicatedWalPosition(wal);
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        ResultColumnDescriptor[] rcds = {
                new GenericColumnDescriptor("walPosition", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
        };
        List<ExecRow> rows = Lists.newArrayList();
        ExecRow template = new ValueRow(1);
        template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
        template.getColumn(1).setValue(walPosition);
        rows.add(template);
        IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
        inprs.openCore();
        resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
    }

    public static void GET_REPLICATED_WAL_POSITIONS(short peerId, ResultSet[] resultSets) throws StandardException, SQLException {
        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
        List<String> walPositions = replicationManager.getReplicatedWalPositions(peerId);
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        ResultColumnDescriptor[] rcds = {
                new GenericColumnDescriptor("walPosition", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
        };
        List<ExecRow> rows = Lists.newArrayList();
        for (String walPosition : walPositions) {
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            template.getColumn(1).setValue(walPosition);
            rows.add(template);
        }
        IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
        inprs.openCore();
        resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
    }
    public static void LIST_PEERS(ResultSet[] resultSets) throws StandardException, SQLException {

        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
        List<ReplicationPeerDescription> replicationPeers = replicationManager.getReplicationPeers();

        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        ResultColumnDescriptor[] rcds = {
                new GenericColumnDescriptor("PeerId", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
                new GenericColumnDescriptor("clusterKey", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
                new GenericColumnDescriptor("enabled", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
                new GenericColumnDescriptor("serial", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN))
        };
        List<ExecRow> rows = Lists.newArrayList();
        for (ReplicationPeerDescription replicationPeer : replicationPeers) {
            ExecRow template = new ValueRow(4);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar(), new SQLBoolean(), new SQLBoolean()});
            template.getColumn(1).setValue(replicationPeer.getId());
            template.getColumn(2).setValue(replicationPeer.getClusterKey());
            template.getColumn(3).setValue(replicationPeer.isEnabled());
            template.getColumn(4).setValue(replicationPeer.isSerial());
            rows.add(template);
        }
        IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
        inprs.openCore();
        resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
    }

    public static void GET_CLUSTER_KEY(ResultSet[] resultSets) throws StandardException, SQLException {

        String clusterKey = getClusterKey();
        resultSets[0] = ProcedureUtils.generateResult("Success", clusterKey);
    }

    public static void ADD_PEER(short peerId,
                                String peerClusterKey,
                                boolean enabled,
                                ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            String clusterKey = getClusterKey();

            // Add peer first
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.addPeer(peerId, peerClusterKey);

            String replicationMonitorQuorum = SIDriver.driver().getConfiguration().getReplicationMonitorQuorum();
            if (replicationMonitorQuorum != null) {
                replicationManager.monitorReplication(clusterKey, peerClusterKey);
            }

            if (enabled) {
                enablePeer(peerId);
            }
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("Added %s as peer %d", peerClusterKey, peerId));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void REMOVE_PEER(short peerId, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.removePeer(peerId);
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("Removed peer %d", peerId));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }


    public static void ENABLE_PEER(short peerId, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            enablePeer(peerId);
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("Enabled peer %d", peerId));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_PEER(short peerId, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.disablePeer(peerId);
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("Disabled peer %d", peerId));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void ENABLE_TABLE_REPLICATION(String schemaName, String tableName, ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("enableTableReplication");
        DataDictionary dd = lcc.getDataDictionary();
        if (dd.databaseReplicationEnabled() ||
                dd.schemaReplicationEnabled(schemaName) ||
                dd.tableReplicationEnabled(schemaName, tableName)) {
            resultSets[0] = ProcedureUtils.generateResult("Warning",
                    String.format("Replication for table %s.%s has been enabled", schemaName, tableName));
            return;
        }
        try {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
            ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                long conglomerate = cd.getConglomerateNumber();
                replicationManager.enableTableReplication(Long.toString(conglomerate));
            }
            dd.addReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, schemaName, tableName), tc);
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for table %s.%s", schemaName, tableName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_TABLE_REPLICATION(String schemaName, String tableName, ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("disableTableReplication");
        DataDictionary dd = lcc.getDataDictionary();
        boolean databaseReplicationEnabled = dd.databaseReplicationEnabled();
        boolean schemaReplicationEnabled = dd.schemaReplicationEnabled(schemaName);
        boolean tableReplicationEnabled = dd.tableReplicationEnabled(schemaName, tableName);

        if (!databaseReplicationEnabled && !schemaReplicationEnabled && !tableReplicationEnabled) {
            resultSets[0] = ProcedureUtils.generateResult("Warning",
                    String.format("Replication for table %s.%s not enabled", schemaName, tableName));
            return;
        }
        try {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
            ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                long conglomerate = cd.getConglomerateNumber();
                replicationManager.disableTableReplication(Long.toString(conglomerate));
            }
            if (tableReplicationEnabled) {
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, schemaName, tableName);
                dd.deleteReplication(d, tc);
            }

            if (schemaReplicationEnabled) {
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, schemaName, null);
                dd.deleteReplication(d, tc);
            }

            if (databaseReplicationEnabled) {
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_DATABASE, null, null);
                dd.deleteReplication(d, tc);
            }

            if (databaseReplicationEnabled || schemaReplicationEnabled) {
                addTableReplication(dd, tc, schemaName, tableName);
            }

            if (databaseReplicationEnabled) {
                addSchemaReplication(dd, tc, schemaName);
            }

            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for table %s.%s", schemaName, tableName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void ENABLE_SCHEMA_REPLICATION(String schemaName, ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("disableSchemaReplication");
        DataDictionary dd = lcc.getDataDictionary();
        if (dd.databaseReplicationEnabled() || dd.schemaReplicationEnabled(schemaName)) {
            resultSets[0] = ProcedureUtils.generateResult("Warning",
                    String.format("Replication for schema %s has been enabled", schemaName));
            return;
        }
        try {
            List<TableDescriptor> tableDescriptors = getTableDescriptorsFromSchema(schemaName);
            enableReplication(tableDescriptors, true);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, schemaName, null), tc);
            dd.addReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, schemaName, null), tc);
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for schema %s", schemaName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_SCHEMA_REPLICATION(String schemaName, ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("disableSchemaReplication");
        DataDictionary dd = lcc.getDataDictionary();
        boolean databaseReplicationEnabled = dd.databaseReplicationEnabled();
        boolean schemaReplicationEnabled = dd.schemaReplicationEnabled(schemaName);

        if (!databaseReplicationEnabled && !schemaReplicationEnabled && !replicationEnabled(schemaName)) {
            resultSets[0] = ProcedureUtils.generateResult("Warning",
                    String.format("Replication for schema %s not enabled", schemaName));
            return;
        }
        try {
            List<TableDescriptor> tableDescriptors = getTableDescriptorsFromSchema(schemaName);
            enableReplication(tableDescriptors, false);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, schemaName, null), tc);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, schemaName, null), tc);
            if (databaseReplicationEnabled) {
                addSchemaReplication(dd, tc, schemaName);
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_DATABASE, null, null);
                dd.deleteReplication(d, tc);
            }
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for schema %s", schemaName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void ENABLE_DATABASE_REPLICATION(ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("enableDatabaseReplication");
        DataDictionary dd = lcc.getDataDictionary();
        if (dd.databaseReplicationEnabled()) {
            resultSets[0] = ProcedureUtils.generateResult("Warning", "Database replication has been enabled");
            return;
        }
        try {
            List<TableDescriptor> tableDescriptors = getTableDescriptorsFromDatabase();
            enableReplication(tableDescriptors, true);
            enableReplicationForSpliceSystemTables();
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, null, null), tc);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, null, null),  tc);
            dd.addReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_DATABASE, null, null), tc);
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for database"));
        } catch (Exception e) {
            StackTraceElement[] stes = e.getStackTrace();
            StringBuffer s = new StringBuffer();
            for (StackTraceElement ste : stes) {
                s.append(ste.toString()).append("\n") ;
            }
            SpliceLogUtils.info(LOG, "%s", s);
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_DATABASE_REPLICATION(ResultSet[] resultSets) throws StandardException, SQLException {
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.getLanguageConnection();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        tc.elevate("disableDatabaseReplication");
        DataDictionary dd = lcc.getDataDictionary();
        if (!replicationEnabled()) {
            resultSets[0] = ProcedureUtils.generateResult("Warning", "Database replication not enabled");
            return;
        }
        try {
            List<TableDescriptor> tableDescriptors = getTableDescriptorsFromDatabase();
            enableReplication(tableDescriptors, false);
            disableReplicationForSpliceSystemTables();
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_DATABASE, null, null), tc);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, null, null), tc);
            dd.deleteReplication(new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, null, null), tc);
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for database"));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void SET_REPLICATION_ROLE(String role, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.setReplicationRole(role);
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("set replication role to '%s'", role));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void GET_REPLICATION_ROLE(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            String role = replicationManager.getReplicationRole();
            resultSets[0] = ProcedureUtils.generateResult("ROLE", String.format("%s", role));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    private static List<String> getSchemas() throws SQLException {
        List<String> schemas = Lists.newArrayList();

        String sql = "select schemaname from sys.sysschemas";
        Connection conn = SpliceAdmin.getDefaultConn();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                schemas.add(rs.getString(1));
            }
            return schemas;
        }
    }

    private static List<String> getTablesFromSchema(String schemaName) throws SQLException{

        List<String> tables = Lists.newArrayList();

        String sql = "select tablename from sys.systables t, sys.sysschemas s " +
                "where t.schemaid=s.schemaid and s.schemaname=?";
        Connection conn = SpliceAdmin.getDefaultConn();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try(ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
                return tables;
            }
        }
    }

    private static List<Pair<String, String>> getTablesFromDatabase() throws SQLException{

        List<Pair<String, String>> tables = Lists.newArrayList();

        String sql = "select schemaname, tablename from sys.systables t, sys.sysschemas s " +
                "where t.schemaid=s.schemaid";
        Connection conn = SpliceAdmin.getDefaultConn();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            try(ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(new Pair(rs.getString(1), rs.getString(2)));
                }
                return tables;
            }
        }
    }

    private static String[] spliceSystemTables = {"16", "SPLICE_TXN", "DROPPED_CONGLOMERATES",
            "SPLICE_CONGLOMERATE", "SPLICE_SEQUENCES"};

    private static void enableReplicationForSpliceSystemTables() throws StandardException, IOException {

        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        for (String table : spliceSystemTables) {
            if (!admin.replicationEnabled(table)) {
                admin.enableTableReplication(table);
            }
        }
    }

    private static void disableReplicationForSpliceSystemTables() throws StandardException, IOException {

        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        for (String table : spliceSystemTables) {
            if (admin.replicationEnabled(table)) {
                admin.disableTableReplication(table);
            }
        }
    }

    private static class EnableReplicationCallable implements Callable<Void> {
        private ReplicationManager replicationManager;
        private long conglomerate;

        public EnableReplicationCallable(ReplicationManager replicationManager,
                                         long conglomerate){
            this.replicationManager = replicationManager;
            this.conglomerate = conglomerate;
        }

        @Override
        public Void call() throws StandardException {
            replicationManager.enableTableReplication(Long.toString(conglomerate));
            return null;
        }
    }

    private static class DisableReplicationCallable implements Callable<Void> {
        private ReplicationManager replicationManager;
        private long conglomerate;

        public DisableReplicationCallable(ReplicationManager replicationManager,
                                         long conglomerate){
            this.replicationManager = replicationManager;
            this.conglomerate = conglomerate;
        }

        @Override
        public Void call() throws StandardException {
            replicationManager.disableTableReplication(Long.toString(conglomerate));
            return null;
        }
    }

    public static String getClusterKey() {
        ConfigurationSource conf = SIDriver.driver().getConfiguration().getConfigSource();
        String zq = conf.getString("hbase.zookeeper.quorum", null);
        String zp = conf.getString("zookeeper.znode.parent", null);
        int port = conf.getInt("hbase.zookeeper.property.clientPort", -1);
        if (!zq.contains(":")) {
            zq = zq + ":" + port;
        }
        String clusterKey = zq + ":" + zp;
        return clusterKey;
    }

    private static void addSchemaReplication(DataDictionary dd,
                                      TransactionController tc,
                                      String schemaName) throws SQLException, StandardException {
        List<String> schemas = getSchemas();
        for (String schema : schemas) {
            if (schema.compareToIgnoreCase(schemaName) != 0) {
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_SCHEMA, schema, null);
                dd.addReplication(d, tc);
            }
        }
    }

    private static void addTableReplication(DataDictionary dd,
                                            TransactionController tc,
                                            String schemaName,
                                            String tableName) throws SQLException, StandardException {
        List<String> tables = getTablesFromSchema(schemaName);
        for (String table : tables) {
            if (table.compareToIgnoreCase(tableName) != 0) {
                ReplicationDescriptor d =
                        new ReplicationDescriptor(SYSREPLICATIONRowFactory.SCOPE_TABLE, schemaName, table);
                dd.addReplication(d, tc);
            }
        }
    }

    private static void enableReplication(List<TableDescriptor> tableDescriptors, boolean enable) throws Exception{
        List<Future<Void>> futures = Lists.newArrayList();
        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        for (TableDescriptor td:tableDescriptors) {
            ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
            for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                long conglomerate = cd.getConglomerateNumber();
                if (enable && !admin.replicationEnabled(Long.toString(conglomerate))) {
                    EnableReplicationCallable callable = new EnableReplicationCallable(replicationManager, conglomerate);
                    Future<Void> future = SIDriver.driver().getExecutorService().submit(callable);
                    futures.add(future);
                } else if (!enable && admin.replicationEnabled(Long.toString(conglomerate))) {
                    DisableReplicationCallable callable = new DisableReplicationCallable(replicationManager, conglomerate);
                    Future<Void> future = SIDriver.driver().getExecutorService().submit(callable);
                    futures.add(future);
                }
            }
        }
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private static List<TableDescriptor> getTableDescriptorsFromDatabase() throws Exception{
        List<Pair<String, String>> tables = getTablesFromDatabase();
        List<TableDescriptor> tableDescriptors = Lists.newArrayList();
        for (Pair<String, String> table : tables) {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(table.getFirst(), table.getSecond());
            tableDescriptors.add(td);
        }
        return tableDescriptors;
    }

    private static List<TableDescriptor> getTableDescriptorsFromSchema(String schemaName) throws Exception{
        List<String> tables = getTablesFromSchema(schemaName);
        List<TableDescriptor> tableDescriptors = Lists.newArrayList();
        for (String table : tables) {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, table);
            tableDescriptors.add(td);
        }
        return tableDescriptors;
    }

    private static boolean replicationEnabled() throws SQLException {

        String sql = "select count(*) from sys.sysreplication";
        Connection conn = SpliceAdmin.getDefaultConn();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            try(ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    private static boolean replicationEnabled(String schemaName) throws SQLException {

        String sql = "select count(*) from sys.sysreplication where scope='TABLE' and schemaname=?";
        Connection conn = SpliceAdmin.getDefaultConn();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try(ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    private static void enablePeer(short peerId) throws StandardException {

        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
        List<ReplicationPeerDescription> replicationPeerDescriptions = replicationManager.getReplicationPeers();
        String peerClusterKey = null;
        boolean enabled = false;
        for (ReplicationPeerDescription replicationPeerDescription : replicationPeerDescriptions) {
            short id = Short.valueOf(replicationPeerDescription.getId());
            if (id == peerId) {
                peerClusterKey = replicationPeerDescription.getClusterKey();
                enabled = replicationPeerDescription.isEnabled();
            }
        }

        if (peerClusterKey == null) {
            throw StandardException.newException(SQLState.INVALID_REPLICATION_PEER, peerId);
        }

        if (enabled)
            return;

        String clusterKey = getClusterKey();
        replicationManager.enablePeer(clusterKey, peerId, peerClusterKey);
    }
 }
