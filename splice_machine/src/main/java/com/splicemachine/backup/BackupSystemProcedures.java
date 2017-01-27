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

package com.splicemachine.backup;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.procedures.ProcedureUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Created by jyuan on 2/12/15.
 */
public class BackupSystemProcedures {

    private static Logger LOG = Logger.getLogger(BackupSystemProcedures.class);

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE
     *
     * @param directory The directory to store a database backup
     * @param type type of backup, either 'FULL' or 'INCREMENTAL'
     * @param resultSets returned results
     * @throws SQLException, StandardException
     */
    public static void SYSCS_BACKUP_DATABASE(String directory, String type, ResultSet[] resultSets)
            throws StandardException, SQLException {

        type = type.trim();
        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        try {
            // Check directory
            if (directory == null || directory.length() == 0) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }

            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check backup type
            if (type.compareToIgnoreCase("FULL") == 0) {
                backupManager.fullBackup(directory);
            } else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {
                backupManager.incrementalBackup(directory);
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
    public static void SYSCS_RESTORE_DATABASE(String directory, long backupId, ResultSet[] resultSets) throws StandardException, SQLException {
        String changeId = null;
        IteratorNoPutResultSet inprs = null;

        Connection conn = SpliceAdmin.getDefaultConn();
        LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            // Check for ongoing backup...
            long runningBackupId = 0;
            if ( (runningBackupId = backupManager.getRunningBackup()) != 0) {
                throw StandardException.newException(SQLState.NO_RESTORE_DURING_BACKUP, runningBackupId);
            }
            backupManager.restoreDatabase(directory,backupId);

            // Print reboot statement
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
                    new GenericColumnDescriptor("result", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 30)),
                    new GenericColumnDescriptor("warnings", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 1024))
            };
            ExecRow template = new ValueRow(2);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();

            Activation activation = lcc.getLastActivation();
            SQLWarning warning = activation.getWarnings();
            while (warning != null) {
                String warningMessage = warning.getLocalizedMessage();
                template.getColumn(1).setValue(warning.getSQLState());
                template.getColumn(2).setValue(warning.getLocalizedMessage());
                rows.add(template.getClone());
                warning = warning.getNextWarning();
            }
            template.getColumn(1).setValue("Restore completed");
            template.getColumn(2).setValue("Database has to be rebooted");
            rows.add(template.getClone());

            inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            LOG.info("Restore completed. Database reboot is required.");

        } catch (Throwable t) {
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
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
            try {
                if (changeId != null) {
                    DDLUtils.finishMetadataChange(changeId);
                }
            } catch (StandardException e) {
                SpliceLogUtils.error(LOG, "Error recovering backup", e);
            }
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        }
    }

    /**
     * Entry point for system procedure SYSCS_SCHEDULE_DAILY_BACKUP. Submit a scheduled job to back up database
     *
     * @param directory A directory in file system to stored backup data
     * @param type type of backup, either 'FULL' or 'INCREMENTAL'
     * @param hour hour of every day(relative to UTC/GMT time) the backup job is executed
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_SCHEDULE_DAILY_BACKUP(final String directory,
                                                   final String type,
                                                   final int hour,
                                                   ResultSet[] resultSets) throws StandardException, SQLException {

        try{
            if (directory == null || directory.length() == 0) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_DIRECTORY, directory);
            }

            if (type.compareToIgnoreCase("FULL") != 0 && type.compareToIgnoreCase("INCREMENTAL") != 0) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_TYPE, type);
            }

            if (hour < 0 || hour >= 24) {
                throw StandardException.newException(SQLState.INVALID_BACKUP_HOUR);
            }
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.scheduleDailyBackup(directory, type, hour);
            resultSets[0] = ProcedureUtils.generateResult("Success",
                                           String.format("Schedule %s daily backup to %s on hour %s", type, directory, hour));
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Schedule daily backup error", t);
        }
    }

    /**
     * Entry point for system procedure SYSCS_CANCEL_DAILY_BACKU\
     * Cancel a scheduled daily backup job
     * @param jobId ID of a backup job to be canceled
     * @param resultSets returned results
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_CANCEL_DAILY_BACKUP(long jobId, ResultSet[] resultSets) throws StandardException, SQLException {

        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.cancelDailyBackup(jobId);
            resultSets[0] = ProcedureUtils.generateResult("Success", "Cancel daily backup "+jobId);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Cancel daily backup error", t);
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
            String sqlText = "select backup_id from sys.sysbackup where begin_timestamp<?";
            try(PreparedStatement ps = conn.prepareStatement(sqlText)){
                ps.setTimestamp(1,ts);
                List<Long> backupIdList=new ArrayList<>();
                BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
                try(ResultSet rs=ps.executeQuery()){
                    while(rs.next()){
                        long backupId=rs.getLong(1);
                        backupIdList.add(backupId);
                    }
                }
                backupManager.removeBackup(backupIdList);
            }
            resultSets[0] = ProcedureUtils.generateResult("Success", "Delete old backups in window "+backupWindow);
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Delete old backups error", t);
        }
    }

    public static void SYSCS_CANCEL_BACKUP(ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            BackupManager backupManager = EngineDriver.driver().manager().getBackupManager();
            backupManager.cancelBackup();
        } catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Cancel backup error", t);
        }
    }


}
