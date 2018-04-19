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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.backup.BackupJobStatus;
import com.splicemachine.backup.BackupRestoreConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.BackupDescriptor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by jyuan on 8/10/16.
 */
public class BackupUtils {
    private static final Logger LOG=Logger.getLogger(BackupUtils.class);

    public static void updateIncrementalChangesAfterSplit(String tableName, Region l, Region r) throws IOException, StandardException {

        // Get all store files from child regions
        Store lStore = l.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
        Store rStore = r.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
        Collection<StoreFile> lStoreFiles = lStore.getStorefiles();
        Collection<StoreFile> rStoreFiles = rStore.getStorefiles();
        Collection<StoreFile> storeFiles = Lists.newArrayList();
        storeFiles.addAll(lStoreFiles);
        storeFiles.addAll(rStoreFiles);
        Configuration conf = HConfiguration.unwrapDelegate();
        FileSystem fs = FSUtils.getCurrentFileSystem(conf);
        String rootDir = FSUtils.getRootDir(conf).toString();
        Path backupPath = new Path(rootDir, BackupRestoreConstants.BACKUP_DIR);
        Path backupMetaPath = new Path(backupPath, BackupRestoreConstants.BACKUP_DATA_DIR);
        Path namespacePath = new Path(backupMetaPath, HConfiguration.getConfiguration().getNamespace());
        Path tablePath = new Path(namespacePath, tableName);

        updateIncrementalChangesForChild(conf, fs, l, tablePath, lStoreFiles);
        updateIncrementalChangesForChild(conf, fs, r, tablePath, rStoreFiles);

        // unregister parent HFile from incremental backup
        for (StoreFile storeFile : storeFiles) {
            String name = storeFile.getPath().getName();
            if (name.indexOf(".") != -1) {
                String[] ns = name.split("\\.");
                Path parentRegionPath = new Path(tablePath, ns[1]);
                Path parentFamilyPath = new Path(parentRegionPath, SIConstants.DEFAULT_FAMILY_NAME);
                Path parentFilePath = new Path(parentFamilyPath, ns[0]);
                if (fs.exists(parentFilePath)) {
                    fs.delete(parentFilePath, true);
                    SpliceLogUtils.info(LOG, "Unregistered %s for incremental backup", parentFilePath.toString());
                }
            }
        }
    }

    /**
     * For each store file in child regions, register it for incremental backup if the parent HFile is registered
     * for incremental backup. The store files in child regions are reference files, which will be materialized
     * by next compaction or incremental backup
     * @param conf
     * @param fs
     * @param r
     * @param tablePath
     * @param storeFiles
     * @throws IOException
     * @throws StandardException
     */
    private static void updateIncrementalChangesForChild(Configuration conf, FileSystem fs, Region r,
                                                         Path tablePath, Collection<StoreFile> storeFiles)
                                                                                throws IOException, StandardException {
        for (StoreFile storeFile : storeFiles) {
            Path p = storeFile.getPath();
            String name = p.getName();
            String regionName = p.getParent().getParent().getName();
            Path regionPath = new Path(tablePath, regionName);

            if (name.indexOf(".") != -1) {
                String[] ns = name.split("\\.");
                SpliceLogUtils.info(LOG, "store file name: %s", p);
                Path parentRegionPath = new Path(tablePath, ns[1]);
                Path parentFamilyPath = new Path(parentRegionPath, SIConstants.DEFAULT_FAMILY_NAME);
                Path parentFilePath = new Path(parentFamilyPath, ns[0]);
                if (fs.exists(parentFilePath)) {
                    registerHFile(conf, fs, regionPath, r, name);
                }
            }
        }
    }

    public static void updateIncrementalChangesAfterCompaction(String tableName,
                                                               String regionName,
                                                               Path resultFile,
                                                               Collection<StoreFile> requestFiles) throws IOException {

        Configuration conf = HConfiguration.unwrapDelegate();
        FileSystem fs = FSUtils.getCurrentFileSystem(conf);
        String rootDir = FSUtils.getRootDir(conf).toString();
        Path backupPath = new Path(rootDir, BackupRestoreConstants.BACKUP_DIR);
        Path backupMetaPath = new Path(backupPath, BackupRestoreConstants.BACKUP_DATA_DIR);
        Path namespacePath = new Path(backupMetaPath, HConfiguration.getConfiguration().getNamespace());
        Path tablePath = new Path(namespacePath, tableName);
        Path regionPath = new Path(tablePath, regionName);
        Path familyPath = new Path(regionPath, SIConstants.DEFAULT_FAMILY_NAME);

        // Get a list of file path to register
        List<Path> pathList = requestFiles.stream().map(new Function<StoreFile, Path>() {
            @Override
            public Path apply(StoreFile storeFile) {
                String fileName = storeFile.getPath().getName();
                return new Path(familyPath, fileName);
            }
        }).collect(Collectors.toList());

        // Register the result file if all request files were registered for incremental backup
        boolean shouldUpdate = true;
        for (Path p : pathList) {
            if (!fs.exists(p)) {
                shouldUpdate = false;
                break;
            }
        }

        if (shouldUpdate) {
            Path r = new Path(familyPath, resultFile.getName());
            Path tempPath = null;
            try {
                // create a temp directory and move all file to it
                tempPath = new Path(familyPath, "tmp");
                fs.mkdirs(tempPath);
                for (Path p : pathList) {
                    fs.rename(p, new Path(tempPath, p.getName()));
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "unregistered HFile %s for incremental backup", p.toString());
                    }
                }

                FSDataOutputStream os = fs.create(r);
                os.close();
                fs.delete(tempPath, true);
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "registered compaction result HFile %s for incremental backup", r.toString());
                }
            }catch(Exception e){
                // restore files from temp directory
                SpliceLogUtils.info(LOG, "Encountered an exception %s when registering compaction result file %s for " +
                        "incremental backup", e.getCause(), r.toString());
                if (tempPath != null && fs.exists(tempPath)) {
                    FileStatus[] statuses = fs.listStatus(tempPath);
                    for (FileStatus fileStatus : statuses) {
                        Path src = fileStatus.getPath();
                        String fileName = src.getName();
                        Path dest = new Path(familyPath, fileName);
                        fs.rename(src, dest);
                        SpliceLogUtils.info(LOG, "rename %s to %s", src.toString(), dest.toString());
                    }
                    fs.delete(tempPath, true);
                }
            }
        }
    }

    public static boolean backupInProgress() throws Exception {
        String path = BackupUtils.getBackupPath();
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        Stat stat = zooKeeper.exists(path, false);
        return (stat != null);
    }
    /**
     *
     * @param fs HBase file system
     * @param rootPath HBase root directory
     * @return True if there exists a successful database backup
     * @throws IOException
     */
    public static boolean existsDatabaseBackup(FileSystem fs, Path rootPath) throws IOException {
        boolean ret = false;
        FSDataInputStream in = null;
        try {
            // Open backup record file from file system
            Path backupPath = new Path(rootPath, BackupRestoreConstants.BACKUP_DIR);
            Path p = new Path(backupPath, BackupRestoreConstants.BACKUP_RECORD_FILE_NAME);
            if (fs.exists(p)) {
                in = fs.open(p);
                int n = in.readInt();  // number of records
                BackupDescriptor bd = new BackupDescriptor();
                while (n-- > 0) {
                    bd.readExternal(in);
                    if (bd.getScope().compareToIgnoreCase("DATABASE") == 0) {
                        ret = true;
                        break;
                    }
                }
            }
            return ret;
        }
        finally {
            if (in != null)
                in.close();
        }
    }

    /**
     * Given a file path for an archived HFile, return the corresponding file path in backup metadata directory
     * @param p file path for archived HFile
     * @return orresponding file path in backup metadata directory
     */
    public static String getBackupFilePath(String p) {
        return p.replace(BackupRestoreConstants.ARCHIVE_DIR, BackupRestoreConstants.BACKUP_DIR);
    }

    public static void waitForBackupToComplete(String tableName, String regionName) throws IOException{
        String path = getBackupPath();
        try {
            List<String> children = ZkUtils.getRecoverableZooKeeper().getChildren(path, false);
            for (String backupId : children) {
                String backupJobPath = path + "/" + backupId;
                String regionBackupPath = backupJobPath + "/" + tableName + "/" + regionName;
                waitForBackupToComplete(tableName, regionName, backupJobPath, regionBackupPath);
            }

        }
        catch (InterruptedException | KeeperException e) {
            throw new IOException(e);
        }
    }

    public static void waitForBackupToComplete(String tableName, String regionName, String backupJobPath, String regionBackupPath) throws IOException{
        int i = 0;
        long maxWaitTime = 60*1000;
        while (regionIsBeingBackup(tableName, regionName, backupJobPath, regionBackupPath)) {
            try {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "wait for backup to complete");
                long waitTime = 100 * (long) Math.pow(2, i);
                if (waitTime <= 0 || waitTime > maxWaitTime) {
                    waitTime = maxWaitTime;
                    i = 0;
                }
                Thread.sleep(waitTime);
                i++;
            }
            catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }


    public static boolean regionIsBeingBackup(String tableName, String regionName, String backupJobPath, String regionBackupPath) {
        boolean isBackup = false;
        try {
            RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
            if (zooKeeper.exists(backupJobPath, false) == null ||
                    backupTimedout()) {
                // Not in backup or backup timed out
                isBackup = false;
            }
            else if (zooKeeper.exists(regionBackupPath, false) == null) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "Table %s region %s is not in backup", tableName, regionName);
                isBackup = false;
            }
            else {
                try {
                    byte[] status = ZkUtils.getData(regionBackupPath);
                    if (Bytes.compareTo(status, HConfiguration.BACKUP_IN_PROGRESS) == 0) {
                        if (LOG.isDebugEnabled())
                            SpliceLogUtils.debug(LOG, "Table %s region %s is in backup", tableName, regionName);
                        isBackup = true;
                    } else if (Bytes.compareTo(status, HConfiguration.BACKUP_DONE) == 0) {
                        if (LOG.isDebugEnabled())
                            SpliceLogUtils.debug(LOG, "Table %s region %s is done with backup", tableName, regionName);
                        isBackup = false;
                    } else {
                        throw new RuntimeException("Unexpected data in node:" + regionBackupPath);
                    }
                } catch ( IOException e) {
                    if (e.getCause() instanceof KeeperException)
                        isBackup = false;
                    else throw e;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return isBackup;
    }

    /**
     * An HFile is eligible for incremental backup if
     * 1) There is an ongoing full backup, flush is not triggered by preparing and backup for this region is done.
     * 2) There is no ongoing backup, AND there is a previous full/incremental backup
     * 3) There is an ongoing incremental backup
     * @param fileName
     * @throws StandardException
     */
    public static void captureIncrementalChanges( Configuration conf,
                                                  HRegion region,
                                                  FileSystem fs,
                                                  Path rootDir,
                                                  Path backupDir,
                                                  String tableName,
                                                  String fileName,
                                                  boolean preparing) throws StandardException {

        if (shouldCaptureIncrementalChanges(fs, rootDir)) {
            registerHFile(conf, fs, backupDir, region, fileName);
        }
    }

    public static boolean shouldCaptureIncrementalChanges(FileSystem fs,Path rootDir) throws StandardException{
        boolean shouldRegister = false;
        try {
            boolean enabled = incrementalBackupEnabled();
            RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
            String spliceBackupPath = HConfiguration.getConfiguration().getBackupPath();
            boolean isRestoreMode = SIDriver.driver().lifecycleManager().isRestoreMode();
            if (enabled && !isRestoreMode) {
                if (BackupUtils.existsDatabaseBackup(fs, rootDir)) {
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "There exists a successful full or incremental backup in the system");
                    }
                    shouldRegister = true;
                } else if (zooKeeper.exists(spliceBackupPath, false) != null) {

                    List<String> backupJobs = zooKeeper.getChildren(spliceBackupPath, false);
                    for (String backupId : backupJobs) {
                        String path = spliceBackupPath + "/" + backupId;
                        byte[] data = zooKeeper.getData(path, false, null);
                        BackupJobStatus status = BackupJobStatus.parseFrom(data);
                        if (status.getScope() == BackupJobStatus.BackupScope.DATABASE) {
                            if (LOG.isDebugEnabled()) {
                                SpliceLogUtils.debug(LOG, "A database backup is running");
                            }
                            shouldRegister = true;
                        }
                    }
                }
            }
            return shouldRegister;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        }
    }

    /**
     * Is this a splice managed table?
     * A table is managed by splice if its namespace is "splice", or it's
     * one of splice system tables
     * @param namespace
     * @param tableName
     * @return
     */
    public static boolean isSpliceTable(String namespace, String tableName) {
        if (namespace == null) {
            if (tableName.compareTo("SPLICE_CONGLOMERATE") == 0 ||
                    tableName.compareTo("SPLICE_SEQUENCES") == 0 ||
                    tableName.compareTo("SPLICE_TXN") == 0 ||
                    tableName.compareTo("TENTATIVE_DDL") == 0) {
                return true;
            }
        }
        else if (namespace.compareTo("splice") == 0)
            return true;

        return false;
    }

    public static boolean backupCanceled() throws KeeperException, InterruptedException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        String path = BackupUtils.getBackupPath();
        return zooKeeper.exists(path, false) == null;
    }

    /**
     * Register this HFile for incremental backup by creating an empty file
     * backup/data/splice/tableName/regionName/V/fileName
     * @param fileName
     * @throws StandardException
     */
    public static void registerHFile(Configuration conf,
                                      FileSystem fs,
                                      Path backupDir,
                                      Region region,
                                      String fileName) throws StandardException {

        FSDataOutputStream out = null;
        try {
            if (!fs.exists(new Path(backupDir, BackupRestoreConstants.REGION_FILE_NAME))) {
                HRegionFileSystem.createRegionOnFileSystem(conf, fs, backupDir.getParent(), region.getRegionInfo());
            }
            Path p = new Path(backupDir.toString() + "/" + SIConstants.DEFAULT_FAMILY_NAME + "/" + fileName);
            // For bulk load, multiple threads may see the same incremental changes and want to register it.
            if (fs.exists(p)) {
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "HFile %s has already been registered for incremental backup", p.toString());
                }
                return;
            }
            out = fs.create(p);
            SpliceLogUtils.info(LOG, "Registered %s for incremental backup", p.toString());
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
        finally {
            try {
                if (out !=null)
                    out.close();
            }
            catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    /**
     * If the end key from the request equals to the region start key, the request is meant for the previous region.
     * Ignore shuch requests.
     * @param request
     * @param region
     * @return
     */
    public static boolean regionKeysMatch(SpliceMessage.PrepareBackupRequest request, HRegion region) {
        byte[] requestStartKey = request.hasStartKey() ? request.getStartKey().toByteArray() : new byte[0];
        byte[] requestEndKey = request.hasEndKey() ? request.getEndKey().toByteArray() : new byte[0];

        HRegionInfo regionInfo = region.getRegionInfo();
        byte[] regionStartKey = regionInfo.getStartKey() != null? regionInfo.getStartKey() : new byte[0];
        byte[] regionEndKey = regionInfo.getEndKey() != null ? regionInfo.getEndKey() : new byte[0];

        return Bytes.compareTo(requestStartKey, regionStartKey) ==0 &&
                Bytes.compareTo(requestEndKey, regionEndKey) == 0;
    }

    private static boolean backupTimedout() throws Exception {
        String path = BackupUtils.getBackupPath();
        boolean timedout = true;

        List<String> backupIds = ZkUtils.getChildren(path, false);
        for (String backupId : backupIds) {
            String backupJobPath = path + "/" + backupId;
            byte[] data = ZkUtils.getData(backupJobPath);
            if (data != null) {
                BackupJobStatus backupJobStatus = null;
                long lastActiveTimestamp = 0;
                try {
                    backupJobStatus = BackupJobStatus.parseFrom(data);
                    lastActiveTimestamp = backupJobStatus.getLastActiveTimestamp();
                    long currentTimestamp = System.currentTimeMillis();
                    long elapsedTime = currentTimestamp - lastActiveTimestamp;
                    long backupTimeout = HConfiguration.getConfiguration().getBackupTimeout();
                    timedout = (lastActiveTimestamp > 0 && elapsedTime > backupTimeout);
                } catch (Exception e) {
                    // The data cannot be parsed. It's either corrupted or a leftover from previous version. In either
                    // case, delete the znode.
                    SpliceLogUtils.info(LOG, "Found a backup znode with unreadable data");
                    timedout = true;
                }
                try {
                    if (timedout) {
                        ZkUtils.recursiveDelete(backupJobPath);
                        SpliceLogUtils.info(LOG, "Found a timeout backup that were active at %s", new Timestamp(lastActiveTimestamp));
                    }
                } catch (KeeperException e) {
                    if (e.code() != KeeperException.Code.NONODE) {
                        SpliceLogUtils.info(LOG, "Znode %s has been removed by another thread", backupJobPath);
                    }
                    else
                        throw e;
                }
            }
        }

        return timedout;
    }

    public static boolean incrementalBackupEnabled() {

        Configuration conf = HConfiguration.unwrapDelegate();
        String fileCleaners = conf.get("hbase.master.hfilecleaner.plugins");
        return fileCleaners.contains("SpliceHFileCleaner");
    }

    public static String getBackupPath() {
        return HConfiguration.getConfiguration().getSpliceRootPath() + HConfiguration.getConfiguration().getBackupPath();
    }

    public static String getBackupLockPath() {
        return HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.DEFAULT_BACKUP_LOCK_PATH;
    }
}
