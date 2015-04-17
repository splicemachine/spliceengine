package com.splicemachine.hbase.backup;

/**
 * Created by jyuan on 3/12/15.
 */

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.storage.TempSplit;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.job.JobFuture;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CancellationException;
import com.google.common.io.Closeables;

import com.splicemachine.utils.SpliceUtilities;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

public class Restore {
    private static Logger LOG = Logger.getLogger(Restore.class);
    private Txn restoreTransaction;
    private List<Backup> backups;
    private Map<String, BackupItem> backupItems;
    private String restoreItemConglomId;

    public Restore() {
    }

    public void setRestoreTransaction(Txn restoreTransaction) {
        this.restoreTransaction = restoreTransaction;
    }

    public Txn getRestoreTransaction() {
        return restoreTransaction;
    }

    public Map<String, BackupItem> getBackupItems() {
        return backupItems;
    }

    /**
     * Create a Restore object
     * @param directory backup directory
     * @param backupId backup ID
     * @return A Restore Object
     * @throws SQLException
     * @throws StandardException
     */
    public static Restore createRestore(String directory, long backupId) throws SQLException, StandardException{

        Restore restore = null;
        try {
            Txn restoreTxn = TransactionLifecycle.getLifecycleManager().
                    beginTransaction().elevateToWritable("recovery".getBytes());

            restore = new Restore();
            restore.setRestoreTransaction(restoreTxn);
            restore.readBackup(directory, backupId);
            Backup lastBackup = restore.getLastBackup();
            lastBackup.restoreProperties();
            lastBackup.restoreMetadata();
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return restore;
    }

    public void restore() throws StandardException {

        HBaseAdmin admin = null;

        try {
            admin = SpliceUtilities.getAdmin();

            // recreate tables
            for (HTableDescriptor table : admin.listTables()) {
                // TODO keep old tables around in case something goes wrong
                //if (table.getNameAsString().compareToIgnoreCase(restoreItemConglomId)!= 0) {
                    admin.disableTable(table.getName());
                    admin.deleteTable(table.getName());
                //}
            }

            Map<String, BackupItem> backUpItems = getBackupItems();
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
                info = new JobInfo(job.getJobId(), future.getNumTasks(), start);
                info.setJobFuture(future);
                try {
                    future.completeAll(info);
                } catch (CancellationException ce) {
                    throw Exceptions.parseException(ce);
                } catch (Throwable t) {
                    info.failJob();
                    throw t;
                }
                completedItems++;
                LOG.info(String.format("Restore progress: %d of %d items restored", completedItems, totalItems));
            }

            // purge transactions
            Backup lastBackup = getLastBackup();
            PurgeTransactionsJob job = new PurgeTransactionsJob(getRestoreTransaction(),
                    lastBackup.getBackupTimestamp(),
                    SpliceAccessManager.getHTable(SpliceConstants.TRANSACTION_TABLE_BYTES));
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(), future.getNumTasks(), start);
            info.setJobFuture(future);
            try {
                future.completeAll(info);
            } catch (CancellationException ce) {
                throw Exceptions.parseException(ce);
            } catch (Throwable t) {
                info.failJob();
                throw t;
            }

            // DB-3089
            restoreTempTable();
            populateRestoreItemsTable(admin);
            deleteSnapshots(admin);
            restoreTransaction.commit();
        } catch (Exception e) {
            try {
                restoreTransaction.rollback();
            } catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }
        finally {
            Closeables.closeQuietly(admin);
        }
    }
    /**
     * Returns the last incremental or full backup.
     * @return
     */
    public Backup getLastBackup() {
        return backups.get(0);
    }

    /**
     * This method mergess two backups b1 and b2 by adding HFile path from b1 to b2
     * @param b1
     * @param b2
     */
    private void mergeBackups(Backup b1, Backup b2) {
        Map<String, BackupItem> backupItems1 = b1.getBackupItems();
        Map<String, BackupItem> backupItems2 = b2.getBackupItems();

        for(String item : backupItems1.keySet()) {
            if (!backupItems2.containsKey(item)) {
                // If b2 does not contain this item, add this item to b2
                backupItems2.put(item, backupItems1.get(item));
            }
            else {
                // If b2 contain this backup item, add each HFile from b1 to the appropriate region of b2
                BackupItem item1 = backupItems1.get(item);
                BackupItem item2 = backupItems2.get(item);
                List<BackupItem.RegionInfo> regionInfoList1 = item1.getRegionInfoList();
                List<BackupItem.RegionInfo> regionInfoList2 = item2.getRegionInfoList();

                for(BackupItem.RegionInfo regionInfo1 : regionInfoList1) {
                    boolean fallsInto = false;
                    for (BackupItem.RegionInfo regionInfo2 : regionInfoList2) {
                        if (regionFallsInto(regionInfo1, regionInfo2)) {
                            // if region2 contains region1, add HFiles from region1 to region 2
                            List<Pair<byte[], String>> famPathList = regionInfo2.getFamPaths();
                            for(Pair<byte[], String> p : regionInfo1.getFamPaths()) {
                                famPathList.add(p);
                            }
                            fallsInto = true;
                            break;
                        }
                    }
                    if (!fallsInto) {
                        regionInfoList2.add(regionInfo1);
                    }
                }

                List<Pair<Long, Long>> ignoreTxns = item1.getIgnoreTxns();
                for (Pair<Long, Long> ignoreTxn : ignoreTxns) {
                    item2.addIgnoreTxn(ignoreTxn);
                }
            }
        }
    }

    /**
     * compare two start keys
     * @param k1 start key
     * @param k2 start key
     * @return 0 if equal; 1 if greater, -1 if less than
     */
    private int compareStartKey(byte[] k1, byte[] k2) {
        int result = 0;
        Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();

        if (k1 == null && k2 == null ||
            k1.length == 0 && k2.length == 0) {
            //Both are null keys
            result = 0;
        }
        else if (k1== null && k2 != null ||
                 k1.length == 0 && k2.length != 0) {
            //key1 is null, key2 non-null, then key1 < key2
            result = -1;
        }
        else if (k1!= null && k2 == null ||
                 k1.length != 0 && k2.length == 0) {
            //key1 is non-null, key2 is null, then key1 > key2
            result = 1;
        }
        else {
            //compare two non-null byte arrays
            result = comparator.compare(k1, k2);
        }

        return result;
    }

    /**
     * Compares two end keys
     * @param k1 end key
     * @param k2 end key
     * @return 0 if equal; 1 if greater, -1 if less than
     */
    private int compareEndKey(byte[] k1, byte[] k2) {
        int result = 0;
        Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();
        if (k1 == null && k2 == null ||
            k1.length == 0 && k2.length == 0) {
            //both are null key
            result = 0;
        }
        else if (k1 == null && k2 != null ||
                 k1.length == 0 && k2.length != 0) {
            //k1 is null, k2 is non-null, k1 > k2
            result = 1;
        }
        else if (k1 != null && k2 == null ||
                 k1.length != 0 && k2.length == 0) {
            //k1 is null, k2 is non-null, k1 < k2
            result = -1;
        }
        else {
            //compare two non-null byte arrays
            result = comparator.compare(k1, k2);
        }

        return result;
    }

    /**
     * Whether region2 contains region1
     * @param regionInfo1
     * @param regionInfo2
     * @return true if region2 contains region1
     */
    private boolean regionFallsInto(BackupItem.RegionInfo regionInfo1, BackupItem.RegionInfo regionInfo2) {
        byte[] startKey1 = regionInfo1.getHRegionInfo().getStartKey();
        byte[] endKey1 = regionInfo1.getHRegionInfo().getEndKey();
        byte[] startKey2 = regionInfo2.getHRegionInfo().getStartKey();
        byte[] endKey2 = regionInfo2.getHRegionInfo().getEndKey();

        return (compareStartKey(startKey1, startKey2) >= 0 && compareEndKey(endKey1, endKey2) <= 0);
    }

    private void readBackupItems() throws StandardException{

        if (backups.size() == 0) {
            throw StandardException.newException("Cannot find backup data for specified directory and backup ID.");
        }
        Iterator<Backup> itr = backups.iterator();
        Backup backup = itr.next();
        while(itr.hasNext()) {
            Backup b = itr.next();
            mergeBackups(backup, b);
            backup = b;
        }
        backupItems = backup.getBackupItems();
    }

    private void readBackup(String directory, long backupId) throws StandardException{
        try {
            createBackups(directory, backupId);
            for (Backup backup : backups) {
                backup.readBackupItems();
            }
            readBackupItems();
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    /**
     * Create Backup records for each incremental and full backups
     * @param directory
     * @param backupId
     * @throws StandardException
     */
    private void createBackups(String directory, long backupId) throws StandardException {

        LinkedList<Backup> backupList = new LinkedList<>();

        try {
            while (directory != null) {
                FileSystem fileSystem = FileSystem.get(URI.create(directory), SpliceConstants.config);
                Path path = new Path(directory + "/BACKUP_" + backupId + "/"+ Backup.BACKUP_META_FOLDER, Backup.PARENT_BACKUP_FILE);
                if (!fileSystem.exists(path)) {
                    directory = null;
                    continue;
                }

                FSDataInputStream in = fileSystem.open(path);
                backupId = in.readLong();
                long parentBackupId = in.readLong();

                Backup backup = new Backup();
                backup.setBackupTransaction(restoreTransaction);
                backup.setBackupId(backupId);
                backup.setParentBackupID(parentBackupId);
                backup.setBackupScope(Backup.BackupScope.D);
                backup.setBackupStatus(Backup.BackupStatus.I);
                backup.setBackupFilesystem(directory);
                backup.setIgnoreTxns();
                backupList.add(backup);
                if (parentBackupId > 0) {
                    directory = in.readUTF();
                    backupId = parentBackupId;

                } else {
                    directory = null;
                }
            }
            backups = backupList;
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    private static void restoreTempTable() throws SQLException {
        TempSplit.SYSCS_SPLIT_TEMP();
    }

    private void populateRestoreItemsTable(HBaseAdmin admin) throws StandardException{

        try {
            for (Backup backup : backups) {
                HashMap<String, BackupItem> backupItems = backup.getBackupItems();
                for (BackupItem backupItem : backupItems.values()) {
                    if (backupItem.getBackupItem().compareToIgnoreCase(SpliceConstants.RESTORE_TABLE_NAME) != 0) {
                        RestoreItem restoreItem = new RestoreItem(backupItem.getBackupItem(),
                                backup.getBackupTimestamp(), backup.getTimestampSource());
                        BackupSystemProcedures.restoreItemReporter.report(restoreItem);
                    }
                }
            }
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    private void deleteSnapshots(HBaseAdmin admin) throws IOException{
        List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();
        for (HBaseProtos.SnapshotDescription s : snapshots){
            admin.deleteSnapshot(s.getName());
        }
    }
}
