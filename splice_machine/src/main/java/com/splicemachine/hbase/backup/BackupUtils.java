package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.*;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import com.splicemachine.si.api.TxnView;
import org.apache.zookeeper.KeeperException;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);

    public static final String BACKUP_FILESET_TABLE = "SYSBACKUPFILESET";
    public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;

    public static String isBackupRunning() throws KeeperException, InterruptedException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        if (zooKeeper.exists(SpliceConstants.DEFAULT_BACKUP_PATH, false) != null) {
            long id = BytesUtil.bytesToLong(zooKeeper.getData(SpliceConstants.DEFAULT_BACKUP_PATH, false, null), 0);
            return String.format("A concurrent backup with id of %d is running.", id);
        }

        return null;
    }

    /**
     *
     * @param region HBase region
     * @return store files for each column family
     * @throws ExecutionException
     */
    public static HashMap<byte[], Collection<StoreFile>> getStoreFiles(HRegion region) throws ExecutionException {
        try {
            HashMap<byte[], Collection<StoreFile>> storeFiles = new HashMap<>();
            Map<byte[],Store> stores = region.getStores();

            for (byte[] family : stores.keySet()) {
                Store store = stores.get(family);
                Collection<StoreFile> storeFileList = store.getStorefiles();
                storeFiles.put(family, storeFileList);
            }
            return storeFiles;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    /**
     * Get directory of the specified backup
     * @param parent_backup_id
     * @return
     * @throws StandardException
     */
    public static String getBackupDirectory(long parent_backup_id, TxnView txn) throws StandardException {

        String dir = null;

        if (parent_backup_id == -1) {
            return null;
        }
        Backup backup = BackupSystemProcedures.backupReporter.getBackup(parent_backup_id, txn);
        if (backup != null) {
            dir = backup.getBackupFilesystem();
        }
        return dir;
    }

    /**
     * Get backup Id for the latest backup
     * @return backup Id
     * @throws SQLException
     */
    public static long  getLastBackupId() throws StandardException {

        long backupId = -1;
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            BackupSystemProcedures.backupReporter.openScanner(txn);
            Backup backup = null;
            Backup.BackupStatus status = Backup.BackupStatus.F;
            do {
                backup = BackupSystemProcedures.backupReporter.next();

                if (backup != null) {
                    backupId = backup.getBackupId();
                    status = backup.getBackupStatus();
                }
                else {
                    backupId = -1;
                }
            } while (backup != null && status != Backup.BackupStatus.S);
            BackupSystemProcedures.backupReporter.closeScanner();
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            } catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }
        return backupId;
    }

    /**
     * Query BACKUP.BACKUP table, return true if there exists an entry with the specified status
     * @param status Back up status, "S", "I", or "F"
     * @return true if there exists an entry with the specified status
     */
    public static boolean existBackupWithStatus(String status) throws StandardException{

        Txn txn = null;
        Backup backup = null;

        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            BackupSystemProcedures.backupReporter.openScanner(txn);
            do {
                backup = BackupSystemProcedures.backupReporter.next();
            } while (backup != null && status.compareToIgnoreCase(backup.getBackupStatus().toString()) != 0);
            BackupSystemProcedures.backupReporter.closeScanner();
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            } catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }

        return backup != null;
    }

    public static String getSnapshotName(String tableName, long backupId) {
        return tableName + "_" + backupId;
    }

    /**
     * Get last snapshot name.
     * @param tableName
     * @return last snapshot name
     */
    public static String getLastSnapshotName(String tableName) {

        String snapshotName = null;
        try {
            long backupId = BackupUtils.getLastBackupId();
            if (backupId > 0) {
                snapshotName = getSnapshotName(tableName, backupId);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.getSnapshotName: %s", e.getMessage());
        }

        return snapshotName;
    }

    /**
     * A reference file should be excluded for next incremental backup if
     * 1) it appears in the last snapshot, or
     * 2) it references
     * @param tableName name of HBase table
     * @param referenceFileName name of reference file
     * @param fs file system
     * @return true, if the file is a reference file, and should not be included in next incremental backup
     * @throws IOException
     */
    public static boolean shouldExcludeReferencedFile(String tableName,
                                                      String referenceFileName,
                                                      FileSystem fs) throws IOException, StandardException{
        String[] s = referenceFileName.split("\\.");
        int n = s.length;
        if (n == 1) {
            // This is not a reference file
            return false;
        }
        String fileName = s[0];
        String encodedRegionName = s[1];

        // Get last snapshot for this table
        SnapshotUtils snapshotUtils = SnapshotUtilsFactory.snapshotUtils;
        Configuration conf = SpliceConstants.config;
        String snapshotName = BackupUtils.getLastSnapshotName(tableName);
        Set<String> pathSet = new HashSet<>();
        if (snapshotName != null) {
            List<Path> pathList = getSnapshotHFileLinksForRegion(snapshotUtils, null, conf, fs, snapshotName);
            for (Path path : pathList) {
                pathSet.add(path.getName());
            }
        }

        if(pathSet.contains(fileName)) {
            // If the referenced file appears in last snapshot, this file should be excluded for next incremental
            // backup
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.info(LOG, "snapshot contains file " + s[0]);
            }
            return true;
        }

        if (shouldExclude(tableName, encodedRegionName, fileName)) {
            // Check BACKUP.BACKUP_FILESET whether this file should be excluded
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.info(LOG, "Find an entry in Backup.Backup_fileset for table = " + tableName +
                        " region = " + s[1] + " file = " + s[0]);
            }
            return true;
        }

        return false;
    }

    /**
     *
     * @param tableName name of table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @return true, if the HFile should not be included in the next incremental backup
     */
    public static boolean shouldExclude(String tableName,
                                        String encodedRegionName,
                                        String fileName) throws StandardException{

        boolean exclude = false;

        BackupFileSetReporter backupFileSetReporter = null;
        BackupFileSet backupFileSet = null;
        try {
            backupFileSetReporter = scanFileSetForRegion(tableName, encodedRegionName);
            while((backupFileSet = backupFileSetReporter.next()) != null) {
                if (backupFileSet.getFileName().compareToIgnoreCase(fileName) == 0 &&
                    backupFileSet.shouldInclude() == false) {
                    exclude = true;
                    break;
                }
            }
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        } finally {
            backupFileSetReporter.closeScanner();
        }

        return exclude;
    }

    /**
     * Insert an entry to BACKUP.BACKUP_FILESET
     * @param tableName name of name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @param include whether this file should be included in next incremental backup
     */
    public static void insertFileSet(String tableName,
                                     String encodedRegionName,
                                     String fileName,
                                     boolean include) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            BackupFileSet backupFileSet = new BackupFileSet(tableName, encodedRegionName, fileName, include);
            BackupSystemProcedures.backupFileSetReporter.report(backupFileSet, txn);
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }
            catch (Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }
    }

    /**
     * Query BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @return
     */
    public static BackupFileSetReporter scanFileSetForRegion(String tableName,
                                                             String encodedRegionName) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction();
            BackupSystemProcedures.backupFileSetReporter.openScanner(txn, tableName, encodedRegionName);
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return BackupSystemProcedures.backupFileSetReporter;
    }

    /**
     * Delete entries from BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @param include whether this file should be included in next incremental backup
     */
    public static void deleteFileSet(String tableName,
                                     String encodedRegionName,
                                     String fileName,
                                     boolean include) throws StandardException{
        Txn txn = null;
        try {
            txn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());
            BackupFileSet backupFileSet = new BackupFileSet(tableName, encodedRegionName, fileName, include);
            BackupSystemProcedures.backupFileSetReporter.remove(backupFileSet, txn);
            txn.commit();
        }
        catch (Exception e) {
            try {
                txn.rollback();
            }catch(Exception ex) {
                throw StandardException.newException(ex.getMessage());
            }
            throw StandardException.newException(e.getMessage());
        }
    }

    /**
     * Look for an entry for an HFile in BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @return
     */
    public static BackupFileSet getFileSet(String tableName, String encodedRegionName, String fileName) {

        BackupFileSetReporter backupFileSetReporter = null;
        BackupFileSet backupFileSet = null;
        try {
            backupFileSetReporter = scanFileSetForRegion(tableName, encodedRegionName);
            while((backupFileSet = backupFileSetReporter.next()) != null) {
                if (backupFileSet.getFileName().compareToIgnoreCase(fileName) == 0) {
                    break;
                }
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.getFileSet: %s", e.getMessage());
        } finally {
            backupFileSetReporter.closeScanner();
        }
        return backupFileSet;
    }

    public static List<Path> getSnapshotHFileLinksForRegion(final SnapshotUtils utils,
                                                    final HRegion region,
                                                    final Configuration conf,
                                                    final FileSystem fs,
                                                    final String snapshotName) throws IOException {
        List<Path> path = new ArrayList<>();
        List<Object> files = utils.getSnapshotFilesForRegion(region, conf, fs, snapshotName);
        for(Object f : files) {
            if (f instanceof HFileLink) {
                path.add(((HFileLink) f).getAvailablePath(fs));
            }
        }
        return path;
    }
}