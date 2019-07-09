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

package com.splicemachine.backup;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.procedures.ProcedureUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Created by jyuan on 2/12/15.
 */
public class BackupSystemProcedures {

    private static Logger LOG = Logger.getLogger(BackupSystemProcedures.class);

    public static void VALIDATE_TABLE_BACKUP(String schemaName,
                                             String tableName,
                                             String directory,
                                             long backupId,
                                             ResultSet[] resultSets) throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        Activation activation = lcc.getLastActivation();
        schemaName = EngineUtils.validateSchema(schemaName);
        tableName = EngineUtils.validateTable(tableName);
        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.validateTableBackup(schemaName, tableName, directory, backupId);
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("Results", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024))
            };
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            SQLWarning warning = activation.getWarnings();
            if (warning == null) {
                template.getColumn(1).setValue("No corruptions found for backup.");
                rows.add(template.getClone());
            }
            else {
                while (warning != null) {
                    String warningMessage = warning.getLocalizedMessage();
                    template.getColumn(1).setValue(warningMessage);
                    rows.add(template.getClone());
                    warning = warning.getNextWarning();
                }
            }

            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Backup validation error", t);
            t.printStackTrace();
        }
    }

    public static void VALIDATE_BACKUP(String directory, long backupId, ResultSet[] resultSets) throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        Activation activation = lcc.getLastActivation();

        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.validateBackup(directory, backupId);
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("Results", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024))
            };
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            SQLWarning warning = activation.getWarnings();
            if (warning == null) {
                template.getColumn(1).setValue("No corruptions found for backup.");
                rows.add(template.getClone());
            }
            else {
                while (warning != null) {
                    String warningMessage = warning.getLocalizedMessage();
                    template.getColumn(1).setValue(warningMessage);
                    rows.add(template.getClone());
                    warning = warning.getNextWarning();
                }
            }

            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Backup validation error", t);
            t.printStackTrace();
        }
    }

    public static void SYSCS_BACKUP_DATABASE(String directory, String type, ResultSet[] resultSets)
            throws StandardException, SQLException {
        type = type.trim();
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        try {
            // Check directory
            if (directory == null || directory.isEmpty()) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }

            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check backup type
            if (type.compareToIgnoreCase("FULL") == 0) {
                backupManager.fullBackup(directory, true);
            } else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {
                backupManager.incrementalBackup(directory, true);
            } else {
                throw StandardException.newException(SQLState.INVALID_BACKUP_TYPE, type);
            }
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("%s backup to %s", type, directory));
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_RESTORE_DATABASE
     * @param directory A directory in file system where backup data are stored
     * @param backupId backup ID
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_RESTORE_DATABASE(String directory, long backupId, boolean validate, ResultSet[] resultSets) throws StandardException, SQLException {
        restoreDatabase(directory, backupId, validate, null, -1, resultSets);
    }

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_RESTORE_DATABASE
     * @param directory A directory in file system where backup data are stored
     * @param backupId backup ID
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_RESTORE_DATABASE_TO_TIMESTAMP(String directory,
                                                           long backupId,
                                                           boolean validate,
                                                           String timestamp,
                                                           ResultSet[] resultSets) throws StandardException, SQLException {
        restoreDatabase(directory, backupId, validate, timestamp, -1, resultSets);
    }

    public static void SYSCS_RESTORE_DATABASE_TO_TRANSACTION(String directory,
                                                           long backupId,
                                                           boolean validate,
                                                           long transactionId,
                                                           ResultSet[] resultSets) throws StandardException, SQLException {
        if (transactionId > backupId) {
            throw StandardException.newException(SQLState.RESTORE_TXNID_TOO_LARGE, transactionId);
        }
        restoreDatabase(directory, backupId, validate, null, transactionId, resultSets);
    }

    private static void restoreDatabase(String directory,
                                        long backupId,
                                        boolean validate,
                                        String timestamp,
                                        long txnId,
                                        ResultSet[] resultSets) throws StandardException, SQLException {
        IteratorNoPutResultSet inprs = null;

        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check for ongoing backup...
            BackupJobStatus[] backupJobStatuses = backupManager.getRunningBackups();
            if ( backupJobStatuses.length > 0) {
                long runningBackupId = backupJobStatuses[0].getBackupId();
                throw StandardException.newException(SQLState.NO_RESTORE_DURING_BACKUP, runningBackupId);
            }
            backupManager.restoreDatabase(directory,backupId, true, validate, timestamp, txnId);

            // Print reboot statement
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("result", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 40)),
                    new GenericColumnDescriptor("warnings", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024))
            };
            ExecRow template = new ValueRow(2);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();

            Activation activation = lcc.getLastActivation();
            SQLWarning warning = activation.getWarnings();
            if (warning != null) {
                while (warning != null) {
                    template.getColumn(1).setValue(warning.getSQLState());
                    template.getColumn(2).setValue(warning.getLocalizedMessage());
                    rows.add(template.getClone());
                    warning = warning.getNextWarning();
                }
                template.getColumn(1).setValue("Found inconsistencies in backup");
                template.getColumn(2).setValue("To force a restore, set valid to false");
                rows.add(template.getClone());
            }
            else {
                template.getColumn(1).setValue("Restore completed");
                template.getColumn(2).setValue("Database has to be rebooted");
                rows.add(template.getClone());
                LOG.info("Restore completed. Database reboot is required.");
            }
            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();

        } catch (Throwable t) {
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("Error", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length()))};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Error recovering backup", t);

        } finally {
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        }
    }

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE
     *
     * @param directory The directory to store a database backup
     * @param type type of backup, either 'FULL' or 'INCREMENTAL'
     * @param resultSets returned results
     * @throws SQLException, StandardException
     */
    public static void SYSCS_BACKUP_DATABASE_ASYNC(String directory, String type, ResultSet[] resultSets)
            throws StandardException, SQLException {

        type = type.trim();
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

        try {
            // Check directory
            if (directory == null || directory.isEmpty()) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }

            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check backup type
            long backupId = 0;
            if (type.compareToIgnoreCase("FULL") == 0) {
                backupId = backupManager.fullBackup(directory, false);
            } else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {
                backupId = backupManager.incrementalBackup(directory, false);
            } else {
                throw StandardException.newException(SQLState.INVALID_BACKUP_TYPE, type);
            }
            resultSets[0] = ProcedureUtils.generateResult("Status", String.format("Launched %s backup %d to %s at background.", type, backupId, directory));
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_RESTORE_DATABASE
     * @param directory A directory in file system where backup data are stored
     * @param backupId backup ID
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_RESTORE_DATABASE_ASYNC(String directory, long backupId, boolean validate, ResultSet[] resultSets) throws StandardException, SQLException {
        IteratorNoPutResultSet inprs = null;

        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check for ongoing backup...
            BackupJobStatus[] backupJobStatuses = backupManager.getRunningBackups();
            if ( backupJobStatuses.length > 0) {
                long runningBackupId = backupJobStatuses[0].getBackupId();
                throw StandardException.newException(SQLState.NO_RESTORE_DURING_BACKUP, runningBackupId);
            }
            backupManager.restoreDatabase(directory,backupId, false, validate, null, -1);

            // Print reboot statement
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("Status", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 200))
            };
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();

            String message = String.format("Launched restore job for backup %d from %s", backupId, directory);
            template.getColumn(1).setValue(message);
            rows.add(template.getClone());

            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            LOG.info(message);

        } catch (Throwable t) {
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("Error", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length()))};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Error recovering backup", t);

        } finally {
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        }
    }

    /**
     * Delete a backup
     * @param backupId Id of a backup to be deleted
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_DELETE_BACKUP(long backupId, ResultSet[] resultSets) throws StandardException, SQLException {
        try{
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            List<Long> backupIds = Lists.newArrayList();
            backupIds.add(new Long(backupId));
            backupManager.removeBackup(backupIds);
            resultSets[0] = ProcedureUtils.generateResult("Success", "Delete backup "+backupId);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Delete backup error", t);
        }
    }

    public static void SYSCS_DELETE_OLD_BACKUPS(int backupWindow, ResultSet[] resultSets) throws StandardException, SQLException {

        Connection conn = SpliceAdmin.getDefaultConn();
        try {
            if (backupWindow < 0) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_WINDOW, backupWindow);
            }

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - backupWindow);
            Timestamp ts = new Timestamp(calendar.getTimeInMillis());

            //Get backups that are more than backupWindow days old
            List<Long> backupIdList=new ArrayList<>();
            String sqlText = "select backup_id, begin_timestamp, incremental_backup from sys.sysbackup order by begin_timestamp desc";
            try(PreparedStatement ps = conn.prepareStatement(sqlText)){
                BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
                try(ResultSet rs=ps.executeQuery()){
                    int fullBackupCount = 0;
                    while(rs.next()){
                        long backupId=rs.getLong(1);
                        Timestamp beginTimestamp = rs.getTimestamp(2);
                        boolean isFullBackup = !rs.getBoolean(3);
                        if (fullBackupCount > 0 && beginTimestamp.compareTo(ts) < 0) {
                            backupIdList.add(backupId);
                        }
                        if (isFullBackup) {
                            fullBackupCount++;
                        }
                    }
                }
                backupManager.removeBackup(backupIdList);
            }
            String message = String.format("Deleted %d old backups for the past %d days", backupIdList.size(), backupWindow);
            resultSets[0] = ProcedureUtils.generateResult("Success", message);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Delete old backups error", t);
        }
    }

    public static void SYSCS_CANCEL_BACKUP(long backupId) throws StandardException, SQLException {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.cancelBackup(backupId);
    }

    public static void SYSCS_GET_RUNNING_BACKUPS(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            Connection conn = SpliceAdmin.getDefaultConn();
            LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            BackupJobStatus[] backupJobStatuses = backupManager.getRunningBackups();
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("BackupId", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                    new GenericColumnDescriptor("Scope", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 10)),
                    new GenericColumnDescriptor("Type", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 20)),
                    new GenericColumnDescriptor("Item", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 20)),
                    new GenericColumnDescriptor("LastActiveTimestamp", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP))
            };
            ExecRow template = new ValueRow(5);
            template.setRowArray(new DataValueDescriptor[]{new SQLLongint(), new SQLVarchar(), new SQLVarchar(),
                    new SQLVarchar(), new SQLTimestamp()});
            List<ExecRow> rows = Lists.newArrayList();
            for (BackupJobStatus backupJobStatus : backupJobStatuses) {
                template.getColumn(1).setValue(backupJobStatus.getBackupId());
                template.getColumn(2).setValue(backupJobStatus.getScope().toString());
                template.getColumn(3).setValue(backupJobStatus.isIncremental()?"Incremental":"Full");
                template.getColumn(4).setValue(backupJobStatus.getObjects().get(0));
                template.getColumn(5).setValue(new Timestamp(backupJobStatus.getLastActiveTimestamp()));

                rows.add(template.getClone());
            }
            IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Cancel backup error", t);
        }
    }

    public static void SYSCS_BACKUP_TABLE(String schemaName,
                                          String tableName,
                                          String directory,
                                          String type,
                                          ResultSet[] resultSets) throws StandardException, SQLException {
        try{
            schemaName = EngineUtils.validateSchema(schemaName);
            tableName = EngineUtils.validateTable(tableName);
            validateTable(schemaName, tableName);
            type = type.trim().toUpperCase();
            if (directory == null || directory.isEmpty()) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            if (type.compareToIgnoreCase("FULL") == 0) {
                backupManager.fullBackupTable(schemaName, tableName, directory);
            }
            else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {

            }
            else {
                throw StandardException.newException(SQLState.INVALID_BACKUP_TYPE, type);
            }
            resultSets[0] = ProcedureUtils.generateResult("Success", String.format("%s backup to %s", type, directory));

        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }


    public static void SYSCS_RESTORE_TABLE(String destSchema,
                                           String destTable,
                                           String sourceSchema,
                                           String sourceTable,
                                           String directory,
                                           long backupId,
                                           boolean validate,
                                           ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            destSchema = EngineUtils.validateSchema(destSchema);
            destTable = EngineUtils.validateTable(destTable);
            sourceSchema = EngineUtils.validateSchema(sourceSchema);
            sourceTable = EngineUtils.validateTable(sourceTable);
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.restoreTable(destSchema, destTable, sourceSchema, sourceTable, directory, backupId, validate);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

    private static void validateTable(String schemaName, String tableName) throws StandardException, SQLException {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
        if (sd == null) {
            throw StandardException.newException(com.splicemachine.db.iapi.reference.SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schemaName);
        }
        TableDescriptor td = dd.getTableDescriptor(tableName, sd, tc);
        if (td == null)
        {
            throw StandardException.newException(com.splicemachine.db.iapi.reference.SQLState.TABLE_NOT_FOUND, tableName);
        }
    }

    public static void SYSCS_BACKUP_SCHEMA(String schemaName,
                                           String directory,
                                           String type,
                                           ResultSet[] resultSets) throws StandardException, SQLException {
        try{
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            Activation activation = lcc.getLastActivation();
            schemaName = EngineUtils.validateSchema(schemaName);
            type = type.trim().toUpperCase();
            if (directory == null || directory.isEmpty()) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            if (type.compareToIgnoreCase("FULL") == 0) {
                backupManager.fullBackupSchema(schemaName, directory);
            }
            else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {

            }
            else {
                throw StandardException.newException(SQLState.INVALID_BACKUP_TYPE, type);
            }
            // Print reboot statement
            ResultColumnDescriptor[] rcds = {
                    new GenericColumnDescriptor("result", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 40)),
                    new GenericColumnDescriptor("warnings", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024))
            };
            ExecRow template = new ValueRow(2);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();

            SQLWarning warning = activation.getWarnings();
            if (warning != null) {
                while (warning != null) {
                    template.getColumn(1).setValue(warning.getSQLState());
                    template.getColumn(2).setValue(warning.getLocalizedMessage());
                    rows.add(template.getClone());
                    warning = warning.getNextWarning();
                }
                IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
                inprs.openCore();
                Connection conn = SpliceAdmin.getDefaultConn();
                resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
            }
            else {
                resultSets[0] = ProcedureUtils.generateResult("Success", String.format("%s backup to %s", type, directory));
            }

        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

    public static void SYSCS_RESTORE_SCHEMA(String destSchema,
                                           String sourceSchema,
                                           String directory,
                                           long backupId,
                                           boolean validate,
                                           ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            destSchema = EngineUtils.validateSchema(destSchema);
            sourceSchema = EngineUtils.validateSchema(sourceSchema);
            //validateTable(destSchema, destTable);
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.restoreSchema(destSchema, sourceSchema, directory, backupId, validate);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

}
