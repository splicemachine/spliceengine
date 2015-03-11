package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HRegionInfo;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);

    public static final String QUERY_LAST_BACKUP = "select max(transaction_id) from %s.%s";
    public static final String BACKUP_FILESET_TABLE = "BACKUP_FILESET";
    public static final String BACKUP_REGIONSET_TABLE = "BACKUP_REGIONSET";
    public static final String INSERT_BACKUP_REGIONSET =
            "insert into %s.%s(backup_item,region_name,parent_region_name) values (?,?,?)" ;

    public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;

    public static String isBackupRunning() throws SQLException {
        return Backup.isBackupRunning();
    }

    public static HashMap<String, Collection<StoreFileInfo>> getStoreFileInfo(HRegion region) throws ExecutionException {
        try {
            HashMap<String, Collection<StoreFileInfo>> storeFileInfo = new HashMap<>();
            HRegionFileSystem hRegionFileSystem = region.getRegionFileSystem();
            Collection<String> families = hRegionFileSystem.getFamilies();
            for (String family : families) {
                Collection<StoreFileInfo> info = hRegionFileSystem.getStoreFiles(family);
                if (info != null) {
                    storeFileInfo.put(family, info);
                }
            }
            return storeFileInfo;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    public static String getBackupDirectory(long parent_backup_id) throws StandardException, SQLException {

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
            throw e;
        } finally {
            if (connection != null)
                connection.close();
        }

        return dir;
    }

    public static long getLastBackupTime() throws SQLException {

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
            SpliceLogUtils.warn(LOG, "cannot query last backup");
        }
        return backupTransactionId;
    }

    public static void recordRegionSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws SQLException{

        HRegion parentRegion = e.getEnvironment().getRegion();
        String tableName = parentRegion.getRegionInfo().getTable().getNameAsString();
        String parentRegionName = parentRegion.getRegionInfo().getEncodedName();
        String lRegionName = l.getRegionInfo().getEncodedName();
        String rRegionName = r.getRegionInfo().getEncodedName();

        recordRegion(tableName, lRegionName, parentRegionName);
        recordRegion(tableName, rRegionName, parentRegionName);
    }

    public static void recordRegion(String backupItem, String regionName, String parentRegionName) throws SQLException{

        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement ps = connection.prepareStatement(
                String.format(BackupUtils.INSERT_BACKUP_REGIONSET, Backup.DEFAULT_SCHEMA, BACKUP_REGIONSET_TABLE));
        ps.setString(1, backupItem);
        ps.setString(2, regionName);
        ps.setString(3, parentRegionName);
        ps.execute();
    }

    /* Write root parent region into backup file system */
    public static void writeParentRegionInfo(String backupDir, String tableName, String encodedRegionName) throws ExecutionException{
        try {
            String parentRegion = null;
            String p = getParentRegion(tableName, encodedRegionName);

            while (p != null) {
                parentRegion = p;
                p = getParentRegion(tableName, p);
            }

            if (parentRegion != null) {
                FileSystem fileSystem = FileSystem.get(URI.create(backupDir), SpliceConstants.config);
                FSDataOutputStream out = fileSystem.create(
                        new Path(backupDir + "/" + encodedRegionName + "/" + "/.parentRegion"));
                out.writeUTF(parentRegion);
                out.close();
            }
        }
        catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private static String getParentRegion(String tableName, String encodedRegionName) {
        String parentRegionName = null;
        Connection connection = null;
        String sqlText = "select parent_region_name from %s.%s where region_name=?";
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, Backup.DEFAULT_SCHEMA, BACKUP_REGIONSET_TABLE));
            ps.setString(1, encodedRegionName);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                parentRegionName = rs.getString(1);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot query backup.backup_regionset");
        }
        return parentRegionName;
    }

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
            SpliceLogUtils.warn(LOG, "cannot query last backup");
        }

        return exists;
    }

    public static String getLastSnapshotName(String tableName) {

        String snapshotName = null;
        Connection connection = null;
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(BackupItem.QUERY_LAST_SNAPSHOTNAME, BackupItem.DEFAULT_SCHEMA, BackupItem.DEFAULT_TABLE));
            ps.setString(1, tableName);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                snapshotName = rs.getString(1);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot query last snapshot name");
        }

        return snapshotName;
    }

    public static boolean shouldExcludeReferencedFile(String tableName, String fileName, FileSystem fs) throws IOException{
        String[] s = fileName.split("\\.");
        int n = s.length;
        if (n == 1)
            return false;
        HBaseAdmin admin = SpliceUtilities.getAdmin();
        String encodedRegionName = s[n-1];
        List<HRegionInfo> regionInfoList = admin.getTableRegions(tableName.getBytes());

        SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
        Configuration conf = SpliceConstants.config;
        String snapshotName = BackupUtils.getLastSnapshotName(tableName);
        Set<String> pathSet = new HashSet<>();
        List<Path> pathList = new ArrayList<>();
        if (snapshotName != null) {
            Path rootDir = FSUtils.getRootDir(conf);
            Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
            pathList = utils.getSnapshotFilesForRegion(null, conf, fs, snapshotDir);
        }

        for (Path path : pathList) {
            pathSet.add(path.getName());
        }
        if(pathSet.contains(s[0])) {
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.info(LOG, "snapshot contains file " + s[0]);
            }
            return true;
        }

        if (shouldExclude(tableName, s[1], s[0])) {
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.info(LOG, "Find an entry in Backup.Backup_fileset for table = " + tableName +
                        " region = " + s[1] + " file = " + s[0]);
            }
            return true;
        }

        return false;
    }
    public static boolean shouldExclude(String tableName, String encodedRegionName, String fileName) {

        boolean exclude = false;
        Connection connection = null;
        String sqlText = "select count(*) from %s.%s where backup_item=? and region_name=? and file_name=? and include=false";

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
            SpliceLogUtils.warn(LOG, "ShouldExcluded: cannot query backup.fileset");
        }
        return exclude;
    }

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
            SpliceLogUtils.warn(LOG, "insertFileSet: cannot insert into backup.fileset");
        }
    }

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
            SpliceLogUtils.warn(LOG, "queryFileSet: cannot query backup.fileset");
        }
        return rs;
    }

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
            SpliceLogUtils.warn(LOG, "deleteFileSet: cannot delete backup.fileset");
        }
    }

    public static void deleteFileSetForRegion(String tableName, String encodedRegionName) {

        Connection connection = null;
        String sqlText = String.format("delete from %s.%s where backup_item=? and region_name=?",
                BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE);
        try {
            if (LOG.isTraceEnabled()) {
                String entry = "(" + tableName + "," + encodedRegionName + ")";
                SpliceLogUtils.info(LOG, "deleteFileSetForRegion: delete " + entry + "from backup.backup_fileset");
            }
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(sqlText);
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.execute();
            ps.close();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "deleteFileSetForRegion: cannot delete from backup.fileset");
        }
    }

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
            SpliceLogUtils.warn(LOG, "getFileSet: cannot query backup.fileset");
        }
        return null;
    }

    // Get a list of parent backup ids
    public static List<Long> getParentBackupIds(long backupId) throws SQLException, StandardException{
        List<Long> parentBackupIdList = new ArrayList<>();
        String sqltext = "select transaction_id, incremental_parent_backup_id from %s.%s where transaction_id<=? order by transaction_id desc";

        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(sqltext, Backup.DEFAULT_SCHEMA, Backup.DEFAULT_TABLE));
            preparedStatement.setLong(1, backupId);
            ResultSet rs = preparedStatement.executeQuery();

            long prev = 0;
            while (rs.next()) {
                long id = rs.getLong(1);
                long parentId = rs.getLong(2);
                if (prev == 0) {
                    if (id != backupId) {
                        throw StandardException.newException("Backup " + backupId + " does not exist");
                    }
                    parentBackupIdList.add(id);
                    prev = parentId;
                }
                else if (id == prev) {
                    parentBackupIdList.add(id);
                    prev = parentId;
                    if (parentId == -1) break;
                }
            }
        } finally {
            if (connection != null)
                connection.close();
        }
        return parentBackupIdList;
    }

    public static void deleteRegionSet() {
        Connection connection = null;
        String sqlText = "delete from %s.%s";
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_REGIONSET_TABLE));
            ps.execute();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "deleteRegionSet: cannot delete backup.backup_regionset");
        }
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