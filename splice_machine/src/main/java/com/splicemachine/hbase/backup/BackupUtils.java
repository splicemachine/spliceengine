package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.*;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.zookeeper.KeeperException;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);

    public static final String QUERY_LAST_BACKUP = "select max(backup_id) from %s.%s";
    public static final String BACKUP_FILESET_TABLE = "BACKUP_FILESET";
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
    public static String getBackupDirectory(long parent_backup_id) throws StandardException {

        if (parent_backup_id == -1) {
            return null;
        }
        String dir = null;
        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(Backup.QUERY_PARENT_BACKUP_DIRECTORY, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            preparedStatement.setLong(1, parent_backup_id);
            ResultSet rs = preparedStatement.executeQuery();

            if (rs.next()) {
                dir = rs.getString(1);
            } else
                throw StandardException.newException("Parent backup does not exist");
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }

        return dir;
    }

    /**
     * Get backup Id for the latest backup
     * @return backup Id
     * @throws SQLException
     */
    public static long  getLastBackupTime() throws SQLException {

        long backupTransactionId = -1;
        Connection connection = null;
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(BackupUtils.QUERY_LAST_BACKUP, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                backupTransactionId = rs.getLong(1);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.getLastBackupTime: %s", e.getMessage());
        }
        return backupTransactionId;
    }

    /**
     * Query BACKUP.BACKUP table, return true if there exists an entry with the specified status
     * @param status Back up status, "S", "I", or "F"
     * @return true if there exists an entry with the specified status
     */
    public static boolean existBackupWithStatus(String status) {
        boolean exists = false;
        Connection connection = null;
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(Backup.RUNNING_CHECK, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            ps.setString(1, status);
            ResultSet rs = ps.executeQuery();
            exists = rs.next();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.existBackupWithStatus: %s", e.getMessage());
        }

        return exists;
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
            long backupId = BackupUtils.getLastBackupTime();
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
                                                      FileSystem fs) throws IOException{
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
    public static boolean shouldExclude(String tableName, String encodedRegionName, String fileName) {

        boolean exclude = false;
        Connection connection = null;
        String sqlText =
                "select count(*) from %s.%s where backup_item=? and region_name=? and file_name=? and include=false";

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                 exclude = (rs.getInt(1) > 0);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.shouldExclude: %s", e.getMessage());
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
    public static void insertFileSet(String tableName, String encodedRegionName, String fileName, boolean include) {
        Connection connection = null;
        String sqlText = String.format("insert into %s.%s (backup_item,region_name,file_name,include) values(?,?,?,?)",
                BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE);

        try {
            if (LOG.isTraceEnabled()) {
                String entry = "(" + tableName + "," + encodedRegionName + "," + fileName + "," + include + ")";
                SpliceLogUtils.info(LOG, "insertFileSet: insert " + entry +
                        "into backup.backup_fileset");
            }
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(sqlText);
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.setBoolean(4, include);
            ps.execute();
            ps.close();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.insertFileSet: %s",e.getMessage());
        }
    }

    /**
     * Query BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param include whether this file should be included in next incremental backup
     * @return
     */
    public static ResultSet queryFileSet(String tableName, String encodedRegionName, boolean include) {

        Connection connection = null;
        ResultSet rs = null;
        String sqlText = "select file_name from %s.%s where backup_item=? and region_name=? and include=?";
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setBoolean(3, include);
            rs = ps.executeQuery();

        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.queryFileSet: %s", e.getMessage());
        }
        return rs;
    }

    /**
     * Delete entries from BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @param include whether this file should be included in next incremental backup
     */
    public static void deleteFileSet(String tableName, String encodedRegionName, String fileName, boolean include) {

        Connection connection = null;
        ResultSet rs = null;
        String sqlText = String.format("delete from %s.%s where backup_item=? and region_name=? and file_name like ? and include=?",
                BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE);
        try {
            if (LOG.isTraceEnabled()) {
                String entry = "(" + tableName + "," + encodedRegionName + "," + fileName + "," + include + ")";
                SpliceLogUtils.info(LOG, "deleteFileSet: delete " + entry + "from backup.backup_fileset");
            }

            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(sqlText);
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.setBoolean(4, include);
            ps.execute();
            ps.close();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.deleteFileSet: %s", e.getMessage());
        }
    }

    /**
     * Look for an entry for an HFile in BACKUP.BACKUP_FILESET
     * @param tableName name of HBase table
     * @param encodedRegionName encoded region name
     * @param fileName name of HFile
     * @return
     */
    public static FileSet getFileSet(String tableName, String encodedRegionName, String fileName) {
        Connection connection = null;
        ResultSet rs = null;
        String sqlText = "select include from %s.%s where backup_item=? and region_name=? and file_name=?";
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            rs = ps.executeQuery();
            if (rs.next()) {
                boolean include = rs.getBoolean(1);
                return new FileSet(tableName, encodedRegionName, fileName, include);
            }

        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "BackupUtils.getFileSet: %s", e.getMessage());
        }
        return null;
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

    public static class FileSet {
        private String tableName;
        private String regionName;
        private String fileName;
        private boolean include;

        public FileSet() {}

        public FileSet(String tableName, String regionName, String fileName, boolean include) {
            this.tableName = tableName;
            this.regionName = regionName;
            this.fileName = fileName;
            this.include = include;
        }

        public String getTableName() {
            return tableName;
        }

        public String getRegionName() {
            return regionName;
        }

        public String getFileName() {
            return fileName;
        }

        public boolean shouldInclude() {
            return include;
        }
    }
}