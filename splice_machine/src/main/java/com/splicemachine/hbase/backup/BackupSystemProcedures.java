package com.splicemachine.hbase.backup;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.jdbc.ClientDriver;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Created by jyuan on 2/12/15.
 */
public class BackupSystemProcedures {

    private static Logger LOG = Logger.getLogger(BackupSystemProcedures.class);
    private static final ScheduledExecutorService backupScheduler;
    private static final Map<Long, ScheduledFuture> backupJobMap;
    private static boolean driverClassLoaded = false;
    private static final String DB_URL_LOCAL = "jdbc:splice://localhost:1527/" + SpliceConstants.SPLICE_DB + ";create=true;user=%s;password=%s";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";
    public static BackupReporter backupReporter;
    public static BackupItemReporter backupItemReporter;
    public static BackupJobReporter backupJobReporter;
    public static BackupFileSetReporter backupFileSetReporter;
    public static RestoreItemReporter restoreItemReporter;

    static {
        ThreadFactory backupFactory = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("backup-scheduler-%d").build();
        backupScheduler = Executors.newScheduledThreadPool(4, backupFactory);
        backupJobMap = new HashMap<Long, ScheduledFuture>();
        backupReporter = new BackupReporter();
        backupItemReporter = new BackupItemReporter();
        backupJobReporter = new BackupJobReporter();
        backupFileSetReporter = new BackupFileSetReporter();
        restoreItemReporter = new RestoreItemReporter();
    }
    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE
     *
     * @param directory The directory to store a database backup
     * @param type type of backup, either 'FULL' or 'INCREMENTAL'
     * @return
     * @throws SQLException, StandardException
     */
    public static void SYSCS_BACKUP_DATABASE(String directory, String type, ResultSet[] resultSets)
            throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = null;
        type = type.trim();
        try {
            conn = SpliceAdmin.getDefaultConn();
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            // Check directory
            if (directory == null || directory.length() == 0) {
                throw StandardException.newException("Invalid backup directory.");
            }

            // Check backup type
            if (type.compareToIgnoreCase("FULL") == 0) {

                backup(directory, -1);

            } else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {
                // Find the most recent backup (full or incremental). The next incremental backup will be based on it.
                long parent_backup_id = BackupUtils.getLastBackupId();
                if (parent_backup_id <= 0) {
                    // Fall back to a full backup
                    backup(directory, -1);
                }
                else {
                    backup(directory, parent_backup_id);
                }
            } else {
                throw StandardException.newException("Incorrect backup type.");
            }
        } catch (Throwable t) {

            DataTypeDescriptor dtd =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length());
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor("Error", dtd)};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Database backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        }
    }

    /**
     * Entry point for system procedure SYSCS_UTIL.SYSCS_RESTORE_DATABASE
     * @param directory A directory in file system where backup data are stored
     * @param backupId backup ID
     * @param resultSets
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_RESTORE_DATABASE(String directory, long backupId, ResultSet[] resultSets) throws StandardException, SQLException {
        String changeId = null;
        LanguageConnectionContext lcc = null;
        Connection conn = null;
        IteratorNoPutResultSet inprs = null;

        try {

            conn = SpliceAdmin.getDefaultConn();
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            // Check for ongoing backup...
            String backupResponse = null;
            if ( (backupResponse = BackupUtils.isBackupRunning()) != null)
                throw new SQLException(backupResponse); // TODO i18n

            Restore restore = Restore.createRestore(directory, backupId);
            // enter restore mode
            DDLChange change = new DDLChange(restore.getRestoreTransaction(), DDLChangeType.ENTER_RESTORE_MODE);
            changeId = DDLCoordinationFactory.getController().notifyMetadataChange(change);

            restore.restore();

            // Print reboot statement
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
                    new GenericColumnDescriptor("result", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 30)),
                    new GenericColumnDescriptor("warnings", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 50))
            };
            ExecRow template = new ValueRow(2);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
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
                    DDLCoordinationFactory.getController().finishMetadataChange(changeId);
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
     * @param resultSets
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_SCHEDULE_DAILY_BACKUP(final String directory,
                                                   final String type,
                                                   final int hour,
                                                   ResultSet[] resultSets) throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = SpliceAdmin.getDefaultConn();

        try {
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            if (directory == null || directory.length() == 0) {
                throw StandardException.newException("Invalid directory name.");
            }

            if (type.compareToIgnoreCase("FULL") != 0 && type.compareToIgnoreCase("INCREMENTAL") != 0) {
                throw StandardException.newException("Invalid backup type.");
            }

            if (hour < 0 || hour >= 24) {
                throw StandardException.newException("Hour must be in range [0, 23].");
            }

            submitBackupJob(directory, type, hour);

        } catch (Throwable t) {


            DataTypeDescriptor dtd =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length());
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor("Error", dtd)};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Schedule daily backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        }
    }

    /**
     * Entry point for system procedure SYSCS_CANCEL_DAILY_BACKU\
     * Cancel a scheduled daily backup job
     * @param jobId ID of a backup job to be canceled
     * @param resultSets
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_CANCEL_DAILY_BACKUP(long jobId, ResultSet[] resultSets)
            throws StandardException, SQLException {

        //Query backup.backup_jobs to validate jobId
        String query = "select * from sys.sysbackupjobs where job_id=?";
        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = SpliceAdmin.getDefaultConn();

        Txn txn = null;
        try {
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            PreparedStatement ps = conn.prepareStatement(query);
            ps.setLong(1, jobId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw StandardException.newException("Invalid job ID.");
            }
            rs.close();

            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());

            ScheduledFuture future = backupJobMap.get(jobId);
            if (future != null) {
                future.cancel(false);
            }
            else {
                SpliceLogUtils.warn(LOG, "Scheduled job with id %d not found", jobId);
            }
            BackupJob backupJob = new BackupJob(jobId);
            backupJobReporter.remove(backupJob, txn);
            txn.commit();

        }
        catch (Throwable t) {

            try{
                if (txn != null) {
                    txn.rollback();
                }
            }
            catch (Exception e) {
                throw StandardException.newException(e.getMessage());
            }

            DataTypeDescriptor dtd =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length());
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor("Error", dtd)};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Cancel daily backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        }
    }

    /**
     * Delete a backup
     * @param backupId Id of a backup to be deleted
     * @param resultSets
     * @throws StandardException
     * @throws SQLException
     */
    public static void SYSCS_DELETE_BACKUP(long backupId, ResultSet[] resultSets)
            throws StandardException, SQLException {
        String query = "select filesystem from sys.sysbackup where backup_id=?";

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = SpliceAdmin.getDefaultConn();

        try {
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            PreparedStatement ps = conn.prepareStatement(query);
            ps.setLong(1, backupId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw StandardException.newException("Invalid backup ID.");
            }
            String filesystem = rs.getString(1);
            rs.close();
            deleteBackup(filesystem, backupId);
        }
        catch (Throwable t) {

            DataTypeDescriptor dtd =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length());
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor("Error", dtd)};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Cancel daily backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        }
    }

    public static void SYSCS_DELETE_OLD_BACKUPS(int backupWindow, ResultSet[] resultSets)
            throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = SpliceAdmin.getDefaultConn();

        try {
            if (backupWindow < 0) {
                throw StandardException.newException("Invalid backup window.");
            }

            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH) - backupWindow);
            Timestamp ts = new Timestamp(calendar.getTimeInMillis());

            //Get backups that are more than backupWindow days old
            String sqlText = "select filesystem, backup_id from sys.sysbackup where begin_timestamp<?";
            PreparedStatement ps = conn.prepareStatement(sqlText);
            ps.setTimestamp(1, ts);
            ResultSet rs = ps.executeQuery();
            List<Long> backupIdList = new ArrayList<>();
            while(rs.next()) {
                String filesystem = rs.getString(1);
                long backupId = rs.getLong(2);
                backupIdList.add(backupId);
                deleteBackup(filesystem, backupId);
                BackupUtils.deleteBackup(backupId);
            }
            rs.close();
        }
        catch (Throwable t) {

            DataTypeDescriptor dtd =
                    DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, t.getMessage().length());
            ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{new GenericColumnDescriptor("Error", dtd)};
            ExecRow template = new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows = Lists.newArrayList();
            template.getColumn(1).setValue(t.getMessage());

            rows.add(template.getClone());
            inprs = new IteratorNoPutResultSet(rows, rcds, lcc.getLastActivation());
            inprs.openCore();
            SpliceLogUtils.error(LOG, "Cancel daily backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        }
    }

    public static void SYSCS_DUMP_RESTORE_ITEMS(ResultSet[] resultSets) throws StandardException, IOException, SQLException {

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = SpliceAdmin.getDefaultConn();
        try {
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
            restoreItemReporter.openScanner();
            RestoreItem restoreItem = null;
            ExecRow template = new ValueRow(3);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar(), new SQLLongint(), new SQLLongint()});
            List<ExecRow> rows = Lists.newArrayList();
            while((restoreItem = restoreItemReporter.next()) != null) {
                template.resetRowArray();
                DataValueDescriptor[] dvds = template.getRowArray();
                dvds[0].setValue(restoreItem.getItem());
                dvds[1].setValue(restoreItem.getBeginTransactionId());
                dvds[2].setValue(restoreItem.getCommitTransactionId());
                rows.add(template.getClone());
            }
            restoreItemReporter.closeScanner();
            ResultColumnDescriptor[] columns = new ResultColumnDescriptor[3];
            columns[0] = new GenericColumnDescriptor("Item",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 32));
            columns[1] = new GenericColumnDescriptor("Begin_Transaction_Id",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columns[2] = new GenericColumnDescriptor("End_Transaction_Id",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            inprs = new IteratorNoPutResultSet(rows, columns, lcc.getLastActivation());
            inprs.openCore();
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class), inprs, false, null, true);
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }

    }
    /*******************************************************************************************************************
     *       Private methods
     * *****************************************************************************************************************
     */


    private static boolean existsSnapshotForBackup(Set<String> snapshotNameSet, long backupId) {
        boolean exists = false;
        String sId = (new Long(backupId)).toString();
        for (String sname : snapshotNameSet) {
            if (sname.endsWith(sId)) {
                exists = true;
                break;
            }
        }
        return exists;

    }
    private static void backup(String backupDir, long parentBackupId)
            throws SQLException, IOException, KeeperException, InterruptedException, StandardException{
        HBaseAdmin admin = null;
        Backup backup = null;
        Set<String> newSnapshotNameSet = new HashSet<>();
        try {
            // Get existing snapshot
            admin = SpliceUtilities.getAdmin();
            List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();
            Set<String> snapshotNameSet = new HashSet<>();
            for (HBaseProtos.SnapshotDescription s : snapshots) {
                snapshotNameSet.add(s.getName());
            }

            // If there is not snapshot, force a full backup
            if (snapshotNameSet.size() == 0 || !existsSnapshotForBackup(snapshotNameSet, parentBackupId)) {
                parentBackupId = -1;
            }

            backup = Backup.createBackup(backupDir, Backup.BackupScope.D, parentBackupId);
            backup.registerBackup();
            backup.createBaseBackupDirectory();
            backup.createBackupItems(admin, snapshotNameSet, newSnapshotNameSet);
            backup.createProperties();
            HashMap<String, BackupItem> backupItems = backup.getBackupItems();
            backup.start();

            for (String key : backupItems.keySet()) {
                BackupItem backupItem =  backupItems.get(key);
                boolean backedUp = backupItem.doBackup();

                if (backedUp) backup.incrementActualBackupCount();
                backup.updateProgress();
            }

            // create metadata, including timestamp source's timestamp
            // this has to be called after all tables have been dumped.
            backup.createMetadata(parentBackupId);
            if (backup.isTemporaryBaseFolder()) {
                backup.moveToBaseFolder();
            }

            for(String snapshotName : snapshotNameSet) {
                admin.deleteSnapshot(snapshotName);
            }
            backup.markBackupSuccessful();
            backup.setEndBackupTimestamp(new Timestamp(System.currentTimeMillis()));
            insertBackup(backup);
            backup.getBackupTransaction().commit();
            backup.deregisterBackup();

        } catch (Throwable e) {

            try {
                if (backup != null) {
                    backup.markBackupFailed();
                    backup.setEndBackupTimestamp(new Timestamp(System.currentTimeMillis()));
                    insertBackup(backup);
                }
            }
            catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            // Delete previous snapshots
            for(String snapshotName : newSnapshotNameSet) {
                admin.deleteSnapshot(snapshotName);
            }
            LOG.error("Couldn't backup database", e);
            throw new SQLException(e.getMessage());
        }finally {
            backup.deregisterBackup();
            Closeables.closeQuietly(admin);
        }
    }

    private static void insertBackup(Backup backup) throws StandardException {
        try {
            backupReporter.report(backup, backup.getBackupTransaction());
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    private static long getInitialDelay(int hour) {
        Calendar calendar = Calendar.getInstance();
        Date date = calendar.getTime();
        int timeZoneOffset = calendar.get(Calendar.ZONE_OFFSET) / (60 * 60 * 1000);
        int dstOffset = calendar.get(Calendar.DST_OFFSET) / (60 * 60 * 1000);
        int adjustedHour = hour + timeZoneOffset +dstOffset;
        if (adjustedHour < 0) {
            adjustedHour += 24;
        }
        Calendar nextCalendar = (Calendar) calendar.clone();
        nextCalendar.set(Calendar.HOUR_OF_DAY, adjustedHour);
        nextCalendar.set(Calendar.MINUTE, 0);
        nextCalendar.set(Calendar.SECOND, 0);
        nextCalendar.set(Calendar.MILLISECOND, 0);

        if (nextCalendar.before(calendar)) {
            int day = nextCalendar.get(Calendar.DAY_OF_MONTH);
            nextCalendar.set(Calendar.DAY_OF_MONTH, day+1);
        }

        Date nextDate = nextCalendar.getTime();
        return (nextDate.getTime() - date.getTime());
    }

    private static void submitBackupJob(final String directory, final String type, int hour) throws StandardException{

        ScheduledFuture future = null;
        Long jobId = null;

        long initialDelay = getInitialDelay(hour);
        long delay = 24*60*60*1000;
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());

            final Connection conn = SpliceAdmin.getDefaultConn();
            future = backupScheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        String sqlText = "call syscs_util.syscs_backup_database('%s', '%s')";
                        Connection connection = getConnection();
                        PreparedStatement ps = connection.prepareStatement(String.format(sqlText, directory, type));
                        ps.execute();
                        conn.commit();
                    } catch (Exception e) {
                        SpliceLogUtils.error(LOG, "Scheduled database backup error", e);
                    }
                }
            }, initialDelay, delay, TimeUnit.MILLISECONDS);

            jobId = txn.getTxnId();
            backupJobMap.put(jobId, future);
            backupJobReporter.report(new BackupJob(jobId, directory, type, hour, new Timestamp(System.currentTimeMillis())), txn);
            txn.commit();
        }
        catch (Exception e) {
            if (jobId != null && backupJobMap.containsKey(jobId)){
                backupJobMap.remove(jobId);
            }

            if (future != null) {
                future.cancel(false);
            }
            try {
                txn.rollback();
            }
            catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }
    }

    private static Connection getConnection() throws SQLException{
        loadDriver();
        return DriverManager.getConnection(getURL(DB_URL_LOCAL, DEFAULT_USER, DEFAULT_USER_PASSWORD), new Properties());
    }

    private static String getURL(String providedURL, String userName, String password) {
        return String.format(providedURL, userName, password);
    }
    private synchronized static void loadDriver() {
        if(!driverClassLoaded) {
            SpliceLogUtils.trace(LOG, "Loading the JDBC Driver");
            try {
                DriverManager.registerDriver(new ClientDriver());
                driverClassLoaded = true;
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to load the JDBC driver.");
            }
        }
    }

    private static void deleteBackup(String directory, long backupId) throws IOException, StandardException{
        Path path = new Path(directory + "/BACKUP_" + backupId);
        FileSystem fs = FileSystem.get(URI.create(path.toString()), SpliceConstants.config);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileStatus[] fileStatuses = fs.listStatus(new Path(directory));
        if (fileStatuses.length == 2) {
            fs.delete(new Path(directory), true);
        }
        BackupUtils.deleteBackup(backupId);
        BackupUtils.deleteBackupItem(backupId);

    }
}
