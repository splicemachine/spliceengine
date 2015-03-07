package com.splicemachine.hbase.backup;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.log4j.Logger;

public class BackupUtils {

    private static final Logger LOG = Logger.getLogger(BackupUtils.class);

    public static final String QUERY_LAST_BACKUP = "select max(transaction_id) from %s.%s";
    public static final String BACKUP_STATE_TABLE = "BACKUP_STATES";
    public static final String BACKUP_FILESET_TABLE = "BACKUP_FILESET";
    public static final String QUERY_BACKUP_STATE = "select region_name from %s.%s where backup_item=? and region_name=? and state=?";
    public static final String DELETE_BACKUP_STATE = "delete from %s.%s where backup_item=? and region_name=? and file_name=?";
    public static final String INSERT_BACKUP_STATE = "insert into %s.%s values(?, ?, ?, ?)";

    public static final String BACKUP_REGION_TABLE = "BACKUP_REGIONS";
    public static final String QUERY_BACKUP_REGION = "select last_backup_timestamp from %s.%s where backup_item=? and region_name=?";
    public static final String INSERT_BACKUP_REGION = "insert into %s.%s (backup_item,region_name,last_backup_timestamp) values (?,?,?)";
    public static final String UPDATE_BACKUP_REGION = "update %s.%s set last_backup_timestamp=? where backup_item=? and region_name=? ";

    public static final String BACKUP_REGIONSET_TABLE = "BACKUP_REGIONSET";
    public static final String QUERY_BACKUP_REGIONSET = "select region_name from %s.%s where backup_item=? and region_name=?";
    public static final String INSERT_BACKUP_REGIONSET =
            "insert into %s.%s(backup_item,region_name,parent_region_name) values (?,?,?)" ;

    public static final String QUERY_REGION_SPLIT = "select region_name, parent_region_name from %s.%s where backup_item=? and (parent_region_name is null or region_name in (select region_name from %s.%s where backup_item=?))";

    public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    public static final String REGION_INFO = "region.info";

    public static String isBackupRunning() throws SQLException {
        return Backup.isBackupRunning();
    }


    /**
     * Write Region to Backup Directory
     *
     * @param region
     * @param backupItem
     * @param backupFileSystem
     * @throws ExecutionException
     */
    public static void fullBackupRegion(HRegion region, BackupItem backupItem, FileSystem backupFileSystem) throws ExecutionException {
        try {
            region.flushcache();
            region.startRegionOperation();
            FileSystem fs = region.getFilesystem();
            String backupDirectory = backupItem.getBackupItemFilesystem();
            updateLastBackupTimestamp(backupItem.getBackupItem(), region);
            FileUtil.copy(fs, derbyFactory.getRegionDir(region), backupFileSystem, new Path(backupDirectory + "/" + derbyFactory.getRegionDir(region).getName()), false, SpliceConstants.config);
            //derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(), new Path(backupDirectory + "/" + derbyFactory.getRegionDir(region).getName() + "/" + REGION_INFO), backupFileSystem, SpliceConstants.config);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        } finally {
            try {
                region.closeRegionOperation();
            } catch (Exception e) {
                throw new ExecutionException(Throwables.getRootCause(e));
            }
        }
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

    private static long getMaxFamilyModificationTime(Collection<StoreFileInfo> storeFileInfo) {
        long t = 0;
        for (StoreFileInfo s : storeFileInfo) {
            t = Math.max(t, s.getModificationTime());
        }
        return t;
    }

    private static long getMaxRegionModificationTime(HashMap<String, Collection<StoreFileInfo>> storeFileInfo) {
        long t = 0;
        for (String key : storeFileInfo.keySet()) {
            Collection<StoreFileInfo> info = storeFileInfo.get(key);
            t = Math.max(t, getMaxFamilyModificationTime(info));
        }
        return t;
    }

    public static long getLastBackupTimestamp(String backupItem, HRegion region) throws SQLException {
        long t = 0;

        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement ps = connection.prepareStatement(
                String.format(QUERY_BACKUP_REGION, Backup.DEFAULT_SCHEMA, BACKUP_REGION_TABLE));
        ps.setString(1, backupItem);
        ps.setString(2, region.getRegionNameAsString());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            t = rs.getLong(1);
        }
        rs.close();
        return t;
    }

    private static void updateLastBackupTimestamp(String backupItem, HRegion region) throws ExecutionException {
        HashMap<String, Collection<StoreFileInfo>> storeFileInfoMap = getStoreFileInfo(region);
        long maxModificationTime = getMaxRegionModificationTime(storeFileInfoMap);
        try {
            long t = getLastBackupTimestamp(backupItem, region);
            Connection connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = null;
            if (t > 0) {
                ps = connection.prepareStatement(
                        String.format(UPDATE_BACKUP_REGION, Backup.DEFAULT_SCHEMA, BACKUP_REGION_TABLE));
                ps.setLong(1, maxModificationTime);
                ps.setString(2, backupItem);
                ps.setString(3, region.getRegionNameAsString());
                ps.execute();
                ps.close();
            } else if (maxModificationTime > 0) {
                ps = connection.prepareStatement(
                        String.format(INSERT_BACKUP_REGION, Backup.DEFAULT_SCHEMA, BACKUP_REGION_TABLE));
                ps.setString(1, backupItem);
                ps.setString(2, region.getRegionNameAsString());
                ps.setLong(3, maxModificationTime);
                ps.execute();
                ps.close();
            }
        } catch (Exception e) {
            throw new ExecutionException(e);
        }

    }

    private static HashSet<String> getExcludedFileSet(HRegion region, String backupItem) throws SQLException {
        HashSet<String> excludedFileSet = new HashSet<>();
        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement ps = connection.prepareStatement(
                String.format(QUERY_BACKUP_STATE, Backup.DEFAULT_SCHEMA, BACKUP_STATE_TABLE));

        ps.setString(1, backupItem);
        ps.setString(2, region.getRegionInfo().getEncodedName());
        ps.setString(3, "EXCLUDE");
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            String name = rs.getString(1);
            excludedFileSet.add(name);
        }

        return excludedFileSet;
    }

    public static void incrementalBackupRegion(HRegion region, BackupItem backupItem, FileSystem backupFileSystem) throws ExecutionException {

        try {
            region.flushcache();
            region.startRegionOperation();

            HashMap<String, Collection<StoreFileInfo>> storeFileInfoMap = getStoreFileInfo(region);
            long lastBackupTimestamp = getLastBackupTimestamp(backupItem.getBackupItem(), region);
            String backupDirectory = backupItem.getBackupItemFilesystem();
            FileSystem fs = region.getFilesystem();
            HashSet<String> excludedFileSet = getExcludedFileSet(region, backupItem.getBackupItem());
            derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(), new Path(backupDirectory), backupFileSystem, SpliceConstants.config);
            for (String family : storeFileInfoMap.keySet()) {
                Collection<StoreFileInfo> storeFileInfo = storeFileInfoMap.get(family);
                for (StoreFileInfo info : storeFileInfo) {
                    long modificationTime = info.getModificationTime();
                    if (modificationTime > lastBackupTimestamp && !excludedFileSet.contains(info.getPath().getName())) {
                        Path srcPath = info.getPath();
                        String s = srcPath.getName();
                        String regionName = derbyFactory.getRegionDir(region).getName();
                        Path destPath = new Path(backupDirectory + "/" + regionName + "/" + family + "/" + s);
                        FileUtil.copy(fs, srcPath, fs, destPath, false, SpliceConstants.config);
                        updateLastBackupTimestamp(backupItem.getBackupItem(), region);
                    }
                }
            }

            // Copy archived HFile
            copyArchivedStoreFile(region, backupItem);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        } finally {
            try {
                region.closeRegionOperation();
            } catch (Exception e) {
                throw new ExecutionException(Throwables.getRootCause(e));
            }
        }
    }

    private static void copyArchivedStoreFile(HRegion region, BackupItem backupItem) throws ExecutionException {

        String backupDirectory = backupItem.getBackupItemFilesystem();
        try {
            Path archiveDir = HFileArchiveUtil.getArchivePath(SpliceConstants.config);
            String encodedRegionName = region.getRegionInfo().getEncodedName();
            String conglomId = backupItem.getBackupItem();
            Path path = new Path(archiveDir + "/data/default/" + backupItem.getBackupItem() + "/" + encodedRegionName);
            FileSystem fileSystem = FileSystem.get(URI.create(path.toString()), SpliceConstants.config);
            FileStatus[] status = fileSystem.listStatus(path);
            for (FileStatus stat : status) {
                //For each column family
                if (!stat.isDirectory()) {
                    continue;
                }
                String family = stat.getPath().getName();
                FileStatus[] fileStatuses = fileSystem.listStatus(stat.getPath());
                for (FileStatus fs : fileStatuses) {
                    Path srcPath = fs.getPath();
                    String fileName = srcPath.getName();
                    Path destPath = new Path(backupDirectory + "/" + encodedRegionName + "/" + family + "/" + fileName);
                    if (retainedForBackup(conglomId, encodedRegionName, fileName)) {
                        FileUtil.copy(fileSystem, srcPath, fileSystem, destPath, false, SpliceConstants.config);
                        deleteBackupStateTable(conglomId, encodedRegionName, fileName);
                    }
                }
            }

        } catch (Exception e) {
            //throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private static void deleteBackupStateTable(String backupItem, String encodedRegionName, String fileName) throws ExecutionException {
        Connection connection = null;

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(DELETE_BACKUP_STATE, Backup.DEFAULT_SCHEMA, BACKUP_STATE_TABLE));
            ps.setString(1, backupItem);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.execute();
        } catch (SQLException e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    public static boolean retainedForBackup(String backupItem, String encodedRegionName, String fileName) {
        Connection connection = null;

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(BackupUtils.QUERY_BACKUP_STATE, Backup.DEFAULT_SCHEMA, BackupUtils.BACKUP_STATE_TABLE));
            ps.setString(1, backupItem);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            return false;
        }

        return false;
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

    public static void recordBackupState(String backupItem, String region, String path, String state) throws SQLException {
        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                String.format(BackupUtils.INSERT_BACKUP_STATE, Backup.DEFAULT_SCHEMA, BACKUP_STATE_TABLE));
        preparedStatement.setString(1, backupItem);
        preparedStatement.setString(2, region);
        preparedStatement.setString(3, path);
        preparedStatement.setString(4, state);
        preparedStatement.execute();
    }

    public static void recordRegionSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws SQLException{

        HRegion parentRegion = e.getEnvironment().getRegion();
        String tableName = parentRegion.getRegionInfo().getTable().getNameAsString();
        String parentRegionName = parentRegion.getRegionNameAsString();
        String lRegionName = l.getRegionNameAsString();
        String rRegionName = r.getRegionNameAsString();

        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement ps = connection.prepareStatement(
                String.format(BackupUtils.QUERY_BACKUP_REGIONSET, Backup.DEFAULT_SCHEMA, BACKUP_REGIONSET_TABLE));
        ps.setString(1, tableName);
        ps.setString(2, parentRegionName);
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            recordRegion(tableName, parentRegionName, null);
        }
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

    public static boolean hasBackup() throws SQLException {
        long t = 0;
        t = getLastBackupTime();
        return t > 0;
    }

    private static void getLeafNodes(RegionSplitTreeNode node, List<RegionSplitTreeNode> list) {

        if (node == null) return;

        RegionSplitTreeNode left = node.getLeft();
        RegionSplitTreeNode right = node.getRight();

        if (left == null && right == null) {
            list.add(node);
            return;
        }

        if (left != null)
            getLeafNodes(left, list);

        if (right != null)
            getLeafNodes(right, list);
    }

    public static List<RegionSplitTreeNode> getLeafNodes(List<RegionSplitTreeNode> nodeList) {
        List<RegionSplitTreeNode> leafNodeList = new ArrayList<>();
        for (RegionSplitTreeNode node : nodeList) {
            getLeafNodes(node, leafNodeList);
        }
        return leafNodeList;
    }

    public static List<RegionSplitTreeNode> getRegionSplitTrees(String backupItem) throws SQLException{

        List<RegionSplitTreeNode> forest = new ArrayList<>();
        Connection connection = SpliceDriver.driver().getInternalConnection();
        PreparedStatement ps = connection.prepareStatement(
                String.format(BackupUtils.QUERY_REGION_SPLIT, Backup.DEFAULT_SCHEMA, BACKUP_REGIONSET_TABLE,
                        Backup.DEFAULT_SCHEMA, BACKUP_REGION_TABLE));
        ps.setString(1, backupItem);
        ps.setString(2, backupItem);

        ResultSet rs = ps.executeQuery();

        // populate a map
        HashMap<String, RegionSplitTreeNode> nodeMap = new HashMap<>();
        while(rs.next()) {
            String regionName = rs.getString(1);
            String parentRegionName = rs.getString(2);
            RegionSplitTreeNode node = new RegionSplitTreeNode(regionName, parentRegionName);
            nodeMap.put(regionName, node);
        }
        rs.close();
        // construct trees for region split
        for(String key : nodeMap.keySet()) {
            RegionSplitTreeNode node = nodeMap.get(key);
            String parentRegionName = node.getParentRegionName();
            RegionSplitTreeNode parent = nodeMap.get(parentRegionName);
            if (parent != null) {
                parent.addChild(node);
            }
        }

        for(String key : nodeMap.keySet()) {
            RegionSplitTreeNode node = nodeMap.get(key);
            if (node.getInDegree() == 0) {
                forest.add(node);
            }
        }
        return forest;
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

    public static boolean shouldExclude(String tableName, String encodedRegionName, String fileName) {

        boolean exclude = false;
        Connection connection = null;
        String sqlText = "select count(*) from %s.%s where backup_item=? and region_name=? and file_name=? and exclude=?";

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.setBoolean(4, false);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                 exclude = (rs.getInt(1) > 0);
            }
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot query backup.fileset");
        }
        return exclude;
    }

    public static void insertFileSet(String tableName, String encodedRegionName, String fileName, boolean include) {
        Connection connection = null;
        String sqlText = "insert into %s.%s (backup_item,region_name,file_name,include) values(?,?,?,?) ";

        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.setBoolean(4, include);
            ps.execute();
            ps.close();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot insert into backup.fileset");
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
            SpliceLogUtils.warn(LOG, "cannot query backup.fileset");
        }
        return rs;
    }

    public static void deleteFileSet(String tableName, String encodedRegionName, String fileName, boolean include) {

        Connection connection = null;
        ResultSet rs = null;
        String sqlText = "delete from %s.%s where backup_item=? and region_name=? and file_name like ? and include=?";
        try {
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, BackupItem.DEFAULT_SCHEMA, BACKUP_FILESET_TABLE));
            ps.setString(1, tableName);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);
            ps.setBoolean(4, include);
            ps.execute();
            ps.close();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG, "cannot query backup.fileset");
        }
    }

    // Get a list of parent backup ids
    public static List<Long> getParentBackupIds(long backupId) throws SQLException{
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

    public static class RegionSplitTreeNode {
        private String regionName;
        private String parentRegionName;
        private RegionSplitTreeNode left, right;
        private int inDegree;

        public RegionSplitTreeNode(String regionName, String parentRegionName) {
            this.regionName = regionName;
            this.parentRegionName = parentRegionName;
            inDegree = 0;
        }

        public void addChild(RegionSplitTreeNode node) {
            if (left == null) {
                left = node;
            } else {
                right = node;
            }
            node.inDegree++;
        }

        public String getRegionName() {
            return regionName;
        }


        public String getParentRegionName() {
            return parentRegionName;
        }

        public int getInDegree() {
            return inDegree;
        }

        public RegionSplitTreeNode getLeft() {
            return left;
        }

        public RegionSplitTreeNode getRight() {
            return right;
        }
    }
}