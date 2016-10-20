/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.hbase;

import com.google.protobuf.Service;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.backup.BackupRestoreConstants;
import com.splicemachine.backup.BackupUtils;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.List;

/**
 * Created by jyuan on 2/18/16.
 */
public class BackupEndpointObserver extends BackupBaseRegionObserver implements CoprocessorService,Coprocessor {
    private static final Logger LOG=Logger.getLogger(BackupEndpointObserver.class);

    private volatile boolean isSplitting;
    private HRegion region;
    private String namespace;
    private String tableName;
    private String regionName;
    private String path;
    private Path backupDir;
    private Configuration conf;
    private FileSystem fs;
    Path rootDir;
    private volatile boolean preparing;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        String[] name = region.getTableDesc().getNameAsString().split(":");
        if (name.length == 2) {
            namespace = name[0];
            tableName = name[1];
        }
        else {
            tableName = name[0];
        }
        regionName = region.getRegionInfo().getEncodedName();

        path = HConfiguration.getConfiguration().getBackupPath() + "/" + tableName + "/" + regionName;
        conf = HConfiguration.unwrapDelegate();
        rootDir = FSUtils.getRootDir(conf);
        fs = FSUtils.getCurrentFileSystem(conf);
        backupDir = new Path(rootDir, BackupRestoreConstants.BACKUP_DIR + "/data/splice/" + tableName + "/" + regionName);
        preparing = false;
    }

    @Override
    public Service getService(){
        return this;
    }


    @Override
    public void prepareBackup(
            com.google.protobuf.RpcController controller,
            SpliceMessage.PrepareBackupRequest request,
            com.google.protobuf.RpcCallback<SpliceMessage.PrepareBackupResponse> done) {

        try {
            preparing = true;
            SpliceMessage.PrepareBackupResponse.Builder responseBuilder = SpliceMessage.PrepareBackupResponse.newBuilder();
            boolean canceled = false;
            if (isSplitting) {
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "%s:%s is being split before trying to prepare for backup", tableName, regionName);
                }
                // return false to client if the region is being split
                responseBuilder.setReadyForBackup(false);
            } else {
                // A region might have been in backup
                if (!regionIsBeingBackup()) {
                   canceled = backupCanceled();
                    if (!canceled) {
                        HBasePlatformUtils.flush(region);
                        // Create a ZNode to indicate that the region is being copied
                        ZkUtils.recursiveSafeCreate(path, HConfiguration.BACKUP_IN_PROGRESS, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
                if (!canceled) {
                    // check again if the region is being split. If so, return an error
                    if (isSplitting) {
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "%s:%s is being split when trying to prepare for backup", tableName, regionName);
                        }
                        responseBuilder.setReadyForBackup(false);
                        //delete the ZNode
                        ZkUtils.recursiveDelete(path);
                    } else {
                        //wait for all compaction and flush to complete
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "%s:%s waits for flush and compaction to complete", tableName, regionName);
                        }
                        region.waitForFlushesAndCompactions();
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "%s:%s is ready for backup", tableName, regionName);
                        }
                        responseBuilder.setReadyForBackup(true);
                    }
                }
                else
                    responseBuilder.setReadyForBackup(false);
            }
            preparing = false;
            done.run(responseBuilder.build());
        } catch (Exception e) {
            controller.setFailed(e.getMessage());
        }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preSplit()");

        waitForBackupToComplete();
        isSplitting = true;
        super.preSplit(e);
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postSplit()");
        isSplitting = false;
        super.postSplit(e, l, r);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preCompactSelection()");
        waitForBackupToComplete();
        super.preCompactSelection(c, store, candidates);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preCompact()");

        waitForBackupToComplete();
        return super.preCompact(e, store, scanner, scanType);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preFlush()");
        waitForBackupToComplete();
        super.preFlush(e);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
        // Register HFiles for incremental backup
        SpliceLogUtils.info(LOG, "Flushing region %s.%s", tableName, regionName);
        try {
            if (namespace.compareTo("splice") != 0)
                return;
            captureIncrementalChanges(resultFile.getPath().getName());
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private void waitForBackupToComplete() throws IOException{
        int i = 0;
        long maxWaitTime = 60*1000;
        while (regionIsBeingBackup()) {
            try {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "wait for backup to complete");
                long waitTime = Math.min(100 * (long) Math.pow(2, i), maxWaitTime);
                Thread.sleep(waitTime);
                i++;
            }
            catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    private boolean regionIsBeingBackup() {
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

    private boolean backupCanceled() throws KeeperException, InterruptedException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        String path = HConfiguration.getConfiguration().getBackupPath();
        return zooKeeper.exists(path, false) == null;
    }

    /**
     * An HFile is eligible for incremental backup if
     * 1) There is an ongoing full backup, flush is not triggered by preparing and backup for this region is done.
     * 2) There is no ongoing backup, AND there is a previous full/incremental backup
     * 3) There is an ongoing incremental backup
     * @param fileName
     * @throws StandardException
     */
    private void captureIncrementalChanges(String fileName) throws StandardException {
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
                registerHFile(fileName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw Exceptions.parseException(e);
        }
    }

    /**
     * Register this HFile for incremental backup by creating an empty file
     * backup/data/splice/tableName/regionName/V/fileName
     * @param fileName
     * @throws StandardException
     */
    private void registerHFile(String fileName) throws StandardException {

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
}
