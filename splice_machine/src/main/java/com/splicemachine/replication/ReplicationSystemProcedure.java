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

package com.splicemachine.replication;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.derby.impl.storage.SpliceRegionAdmin;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.procedures.ProcedureUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;
import java.sql.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by jyuan on 2/8/19.
 */
public class ReplicationSystemProcedure {

    private static Logger LOG = Logger.getLogger(ReplicationSystemProcedure.class);

    public static void ADD_PEER(short peerId, String clusterKey, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.addPeer(peerId, clusterKey);
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("Added %s as peer %d", clusterKey, peerId));
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
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            replicationManager.enablePeer(peerId);
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
        try {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
            ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                long conglomerate = cd.getConglomerateNumber();
                replicationManager.enableTableReplication(Long.toString(conglomerate));
            }
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for table %s.%s", schemaName, tableName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_TABLE_REPLICATION(String schemaName, String tableName, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
            ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
            ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
            for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                long conglomerate = cd.getConglomerateNumber();
                replicationManager.disableTableReplication(Long.toString(conglomerate));
            }
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for table %s.%s", schemaName, tableName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void ENABLE_SCHEMA_REPLICATION(String schemaName, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            List<String> tables = getTablesFromSchema(schemaName);
            List<Future<Void>> futures = Lists.newArrayList();
            for (String tableName:tables) {
                TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
                ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
                ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
                for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                    long conglomerate = cd.getConglomerateNumber();
                    EnableReplicationCallable callable = new EnableReplicationCallable(replicationManager, conglomerate);
                    Future<Void> future = SIDriver.driver().getExecutorService().submit(callable);
                    futures.add(future);
                }
            }
            for (Future<Void> future : futures) {
                future.get();
            }
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for schema %s", schemaName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_SCHEMA_REPLICATION(String schemaName, ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            List<Future<Void>> futures = Lists.newArrayList();
            List<String> tables = getTablesFromSchema(schemaName);
            for (String tableName:tables) {
                TableDescriptor td = SpliceRegionAdmin.getTableDescriptor(schemaName, tableName);
                ConglomerateDescriptor[] conglomerateDescriptors = td.getConglomerateDescriptors();
                ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();
                for (ConglomerateDescriptor cd : conglomerateDescriptors) {
                    long conglomerate = cd.getConglomerateNumber();
                    DisableReplicationCallable callable = new DisableReplicationCallable(replicationManager, conglomerate);
                    Future<Void> future = SIDriver.driver().getExecutorService().submit(callable);
                    futures.add(future);
                }
            }
            for (Future<Void> future : futures) {
                future.get();
            }
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for schema %s", schemaName));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void ENABLE_DATABASE_REPLICATION(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            List<String> schemas = getSchemas();
            for (String schema : schemas) {
                ENABLE_SCHEMA_REPLICATION(schema, resultSets);
            }
            enableReplicationForSpliceSystemTables();
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Disabled replication for database"));
        } catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void DISABLE_DATABASE_REPLICATION(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            List<String> schemas = getSchemas();
            for (String schema : schemas) {
                DISABLE_SCHEMA_REPLICATION(schema, resultSets);
            }
            disableReplicationForSpliceSystemTables();
            resultSets[0] = ProcedureUtils.generateResult(
                    "Success", String.format("Enabled replication for database"));
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
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            schemas.add(rs.getString(1));
        }
        return schemas;
    }

    private static List<String> getTablesFromSchema(String schemaName) throws SQLException{

        List<String> tables = Lists.newArrayList();

        String sql = "select tablename from sys.systables t, sys.sysschemas s " +
                "where t.schemaid=s.schemaid and s.schemaname=?";
        Connection conn = SpliceAdmin.getDefaultConn();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, schemaName);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            tables.add(rs.getString(1));
        }

        return tables;
    }

    private static void enableReplicationForSpliceSystemTables() throws StandardException {

        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();

        replicationManager.enableTableReplication("16");
        replicationManager.enableTableReplication("SPLICE_TXN");
        replicationManager.enableTableReplication("DROPPED_CONGLOMERATES");
        replicationManager.enableTableReplication("SPLICE_CONGLOMERATE");
        replicationManager.enableTableReplication("SPLICE_SEQUENCES");
    }

    private static void disableReplicationForSpliceSystemTables() throws StandardException {

        ReplicationManager replicationManager = EngineDriver.driver().manager().getReplicationManager();

        replicationManager.disableTableReplication("16");
        replicationManager.disableTableReplication("SPLICE_TXN");
        replicationManager.disableTableReplication("DROPPED_CONGLOMERATES");
        replicationManager.disableTableReplication("SPLICE_CONGLOMERATE");
        replicationManager.disableTableReplication("SPLICE_SEQUENCES");
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
}