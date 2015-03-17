package com.splicemachine.hbase.backup;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLCoordinationFactory;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.job.JobFuture;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.Exceptions;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * Created by jyuan on 2/12/15.
 */
public class BackupSystemProcedures {

    private static Logger LOG = Logger.getLogger(BackupSystemProcedures.class);

    public static void SYSCS_BACKUP_DATABASE(String directory, String type, int frequency, ResultSet[] resultSets)
            throws StandardException, SQLException {

        IteratorNoPutResultSet inprs = null;
        LanguageConnectionContext lcc = null;
        Connection conn = null;
        try {
            conn = SpliceAdmin.getDefaultConn();
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            if (directory == null || directory.length() == 0) {
                throw StandardException.newException("Invalid backup directory.");
            }

            if (frequency != -1) {
                throw StandardException.newException("Scheduled database backup is not supported.");
            }
            if (type.compareToIgnoreCase("FULL") == 0) {
                backup(directory, -1);
            } else if (type.compareToIgnoreCase("INCREMENTAL") == 0) {
                long parent_backup_id = BackupUtils.getLastBackupTime();
                if (parent_backup_id == 0) {
                    throw StandardException.newException("Cannot find a full backup.");
                }
                backup(directory, parent_backup_id);
            } else {
                throw StandardException.newException("Incorrect backup type.");
            }
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
            SpliceLogUtils.error(LOG, "Database backup error", t);
            resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        }
    }

    private static void backup(String backupDir, long parent_backup_id) throws SQLException, IOException{
        HBaseAdmin admin = null;
        Backup backup = null;
        Set<String> newSnapshotNameSet = new HashSet<>();
        int count = 0;
        try {

            // Check for ongoing backup...
            // TODO: Use zookeeper to make sure only one backup or restore job is executing
            String backupResponse = null;
            if ( (backupResponse = BackupUtils.isBackupRunning()) != null)
                throw new SQLException(backupResponse); // TODO i18n

            backup = Backup.createBackup(backupDir, Backup.BackupScope.D, parent_backup_id);
            backup.createBaseBackupDirectory();
            admin = SpliceUtilities.getAdmin();
            List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();

            Set<String> snapshotNameSet = new HashSet<>();
            for (HBaseProtos.SnapshotDescription s : snapshots) {
                snapshotNameSet.add(s.getName());
            }
            backup.createBackupItems(admin, snapshotNameSet, newSnapshotNameSet);
            backup.insertBackup();
            backup.createProperties();
            HashMap<String, BackupItem> backupItems = backup.getBackupItems();

            for (String key : backupItems.keySet()) {
                BackupItem backupItem =  backupItems.get(key);
                boolean backedUp = backupItem.doBackup();
                if (backedUp)
                    count++;
            }

            // create metadata, including timestamp source's timestamp
            // this has to be called after all tables have been dumped.
            backup.createMetadata();

            if (backup.isTemporaryBaseFolder()) {
                backup.moveToBaseFolder();
            }

            for(String snapshotName : snapshotNameSet) {
                admin.deleteSnapshot(snapshotName);
            }
            backup.markBackupSuccesful();
            backup.writeBackupStatusChange(count);
        } catch (Throwable e) {
            if (backup != null) {
                backup.markBackupFailed();
                backup.writeBackupStatusChange(count);
            }
            for(String snapshotName : newSnapshotNameSet) {
                admin.deleteSnapshot(snapshotName);
            }
            LOG.error("Couldn't backup database", e);
            throw new SQLException(Exceptions.parseException(e));
        }finally {
            Closeables.closeQuietly(admin);
        }
    }

    public static void SYSCS_RESTORE_DATABASE(long backupId, ResultSet[] resultSets) throws StandardException, SQLException {
        HBaseAdmin admin = null;
        String changeId = null;
        LanguageConnectionContext lcc = null;
        Connection conn = null;
        IteratorNoPutResultSet inprs = null;
        String restoreDir = null;
        try {
            admin = SpliceUtilities.getAdmin();

            conn = SpliceAdmin.getDefaultConn();
            lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();

            // Check for ongoing backup...
            String backupResponse = null;
            if ( (backupResponse = BackupUtils.isBackupRunning()) != null)
                throw new SQLException(backupResponse); // TODO i18n

            Restore restore = Restore.createRestore(backupId);
            // enter restore mode
            DDLChange change = new DDLChange(restore.getRestoreTransaction(), DDLChangeType.ENTER_RESTORE_MODE);
            changeId = DDLCoordinationFactory.getController().notifyMetadataChange(change);

            // recreate tables
            for (HTableDescriptor table : admin.listTables()) {
                // TODO keep old tables around in case something goes wrong
                admin.disableTable(table.getName());
                admin.deleteTable(table.getName());
            }

            Map<String, BackupItem> backUpItems = restore.getBackupItems();
            for (String key : backUpItems.keySet()) {
                BackupItem backupItem = backUpItems.get(key);
                backupItem.recreateItem(admin);
            }

            JobFuture future = null;
            JobInfo info = null;
            long start = System.currentTimeMillis();
            int totalItems = backUpItems.size();
            int completedItems = 0;
            // bulk import the regions
            for (String key : backUpItems.keySet()) {
                BackupItem backupItem = backUpItems.get(key);
                HTableInterface table = SpliceAccessManager.getHTable(backupItem.getBackupItemBytes());
                RestoreBackupJob job = new RestoreBackupJob(backupItem, table);
                future = SpliceDriver.driver().getJobScheduler().submit(job);
                info = new JobInfo(job.getJobId(),future.getNumTasks(), start);
                info.setJobFuture(future);
                try{
                    future.completeAll(info);
                }catch(CancellationException ce){
                    throw Exceptions.parseException(ce);
                }catch(Throwable t){
                    info.failJob();
                    throw t;
                }
                completedItems++;
                LOG.info(String.format("Restore progress: %d of %d items restored", completedItems, totalItems));
            }

            // purge transactions
            Backup lastBackup = restore.getLastBackup();
            PurgeTransactionsJob job = new PurgeTransactionsJob(restore.getRestoreTransaction(),
                    lastBackup.getBackupTimestamp(),
                    SpliceAccessManager.getHTable(SpliceConstants.TRANSACTION_TABLE_BYTES) );
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(),future.getNumTasks(), start);
            info.setJobFuture(future);
            try{
                future.completeAll(info);
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }catch(Throwable t){
                info.failJob();
                throw t;
            }

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
            Closeables.closeQuietly(admin);
        }
    }

}
