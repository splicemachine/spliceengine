package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.backup.BackupRestoreConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.BackupDescriptor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

/**
 * Created by jyuan on 8/10/16.
 */
public class BackupUtils {
    private static final Logger LOG=Logger.getLogger(BackupUtils.class);


    public static SpliceMessage.PrepareBackupResponse.Builder prepare(SpliceMessage.PrepareBackupRequest request,
                                                                      HRegion region, boolean isSplitting,
                                                                      boolean isCompacting, boolean isFlushing,
                                                                      String tableName, String regionName, String path) throws Exception{

            SpliceMessage.PrepareBackupResponse.Builder responseBuilder = SpliceMessage.PrepareBackupResponse.newBuilder();
            responseBuilder.setReadyForBackup(false);

            if (shouldIgnore(request, region)) {
                return responseBuilder;
            }

            boolean canceled = false;

            if (isSplitting) {
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "%s:%s is being split before trying to prepare for backup", tableName, regionName);
                }
                // return false to client if the region is being split
                responseBuilder.setReadyForBackup(false);
            } else {
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "%s:%s waits for flush and compaction to complete", tableName, regionName);
                }

                // A region might have been in backup. This is unlikely to happen unless the previous response ws lost
                // and the client is retrying
                if (!BackupUtils.regionIsBeingBackup(tableName, regionName, path)) {
                    // Flush memsotore and Wait for flush and compaction to be done
                    HBasePlatformUtils.flush(region);
                    region.waitForFlushesAndCompactions();

                    canceled = BackupUtils.backupCanceled();
                    if (!canceled) {
                        // Create a ZNode to indicate that the region is being copied
                        ZkUtils.recursiveSafeCreate(path, HConfiguration.BACKUP_IN_PROGRESS, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG,"create node %s to mark backup in progress", path);
                        }

                        if (isFlushing || isCompacting || isSplitting) {
                            if (LOG.isDebugEnabled()) {
                                SpliceLogUtils.debug(LOG, "table %s region %s is not ready for backup: isSplitting=%s, isCompacting=%s, isFlushing=%s",
                                        isSplitting, isCompacting, isFlushing);
                            }
                            ZkUtils.recursiveDelete(path);
                        }
                        else {
                            responseBuilder.setReadyForBackup(true);
                            if (LOG.isDebugEnabled()) {
                                SpliceLogUtils.debug(LOG, "%s:%s is ready for backup", tableName, regionName);
                            }
                        }
                    }
                }
                else
                    responseBuilder.setReadyForBackup(true);
            }
            return responseBuilder;
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

    public static void waitForBackupToComplete(String tableName, String regionName, String path) throws IOException{
        int i = 0;
        long maxWaitTime = 60*1000;
        while (regionIsBeingBackup(tableName, regionName, path)) {
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


    public static boolean regionIsBeingBackup(String tableName, String regionName, String path) {
        boolean isBackup = false;
        try {
            RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
            if (zooKeeper.exists(path, false) == null) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "Table %s region %s is not in backup", tableName, regionName);
                isBackup = false;
            }
            else {
                byte[] status = ZkUtils.getData(path);
                if (Bytes.compareTo(status, HConfiguration.BACKUP_IN_PROGRESS) == 0) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, "Table %s region %s is in backup", tableName, regionName);
                    isBackup = true;
                }
                else if (Bytes.compareTo(status, HConfiguration.BACKUP_DONE) == 0) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, "Table %s region %s is done with backup", tableName, regionName);
                    isBackup = false;
                }
                else {
                    throw new RuntimeException("Unexpected data in node:" + path);
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
                                                  String path,
                                                  FileSystem fs,
                                                  Path rootDir,
                                                  Path backupDir,
                                                  String tableName,
                                                  String fileName,
                                                  boolean preparing) throws StandardException {
        boolean shouldRegister = false;
        try {
            RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
            String spliceBackupPath = HConfiguration.getConfiguration().getBackupPath();
            if (zooKeeper.exists(spliceBackupPath, false) != null) {
                byte[] backupType = ZkUtils.getData(spliceBackupPath);
                if (Bytes.compareTo(backupType, BackupRestoreConstants.BACKUP_TYPE_FULL_BYTES) == 0) {
                    if (!preparing && zooKeeper.exists(path, false) != null) {
                        byte[] status = ZkUtils.getData(path);
                        if (Bytes.compareTo(status, HConfiguration.BACKUP_DONE) == 0) {
                            if (LOG.isDebugEnabled()) {
                                SpliceLogUtils.debug(LOG, "Table %s is being backup", tableName);
                            }
                            shouldRegister = true;
                        }
                    }
                }
                else {
                    shouldRegister = true;
                }

            }
            else if (BackupUtils.existsDatabaseBackup(fs, rootDir)) {
                shouldRegister = true;
            }
            if (shouldRegister) {
                registerHFile(conf, fs, backupDir, region, fileName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw Exceptions.parseException(e);
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
        String path = HConfiguration.getConfiguration().getBackupPath();
        return zooKeeper.exists(path, false) == null;
    }

    /**
     * Register this HFile for incremental backup by creating an empty file
     * backup/data/splice/tableName/regionName/V/fileName
     * @param fileName
     * @throws StandardException
     */
    private static void registerHFile(Configuration conf,
                                      FileSystem fs,
                                      Path backupDir,
                                      HRegion region,
                                      String fileName) throws StandardException {

        FSDataOutputStream out = null;
        try {
            if (!fs.exists(new Path(backupDir, BackupRestoreConstants.REGION_FILE_NAME))) {
                HRegionFileSystem.createRegionOnFileSystem(conf, fs, backupDir.getParent(), region.getRegionInfo());
            }
            out = fs.create(new Path(backupDir.toString() + "/" + SIConstants.DEFAULT_FAMILY_NAME + "/" + fileName));
        }
        catch (Exception e) {
            throw Exceptions.parseException(e);
        }
        finally {
            try {
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
    private static boolean shouldIgnore(SpliceMessage.PrepareBackupRequest request, HRegion region) {
        byte[] endKey = request.hasEndKey() ? request.getEndKey().toByteArray() : null;
        byte[] regionStartKey = region.getRegionInfo().getStartKey();
        if (endKey != null && endKey.length > 0 && Bytes.compareTo(endKey, regionStartKey) == 0)
            return true;

        return false;
    }
}
