/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.google.protobuf.ByteString;
import com.google.protobuf.Service;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.backup.BackupMessage;
import com.splicemachine.backup.BackupRestoreConstants;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.DummyScanner;
import com.splicemachine.backup.BackupMessage.BackupRegionStatus;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jyuan on 2/18/16.
 */
public class BackupEndpointObserver extends BackupMessage.BackupCoprocessorService implements RegionCoprocessor, RegionObserver {
    private static final Logger LOG=Logger.getLogger(BackupEndpointObserver.class);

    private AtomicBoolean isSplitting;
    private AtomicBoolean isCompacting;
    private HRegion region;
    private String namespace;
    private String tableName;
    private String regionName;
    private Path backupDir;
    private Configuration conf;
    private FileSystem fs;
    private Path rootDir;
    private AtomicBoolean preparing;
    private ThreadLocal<Collection<? extends StoreFile>> storeFiles = new ThreadLocal<Collection<? extends StoreFile>>();
    private ReentrantLock bulkLoadLock = new ReentrantLock();
    protected Optional<RegionObserver> optionalRegionObserver = Optional.empty();

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        try {
            optionalRegionObserver = Optional.of(this);
            region = (HRegion) ((RegionCoprocessorEnvironment) e).getRegion();
            String[] name = region.getTableDescriptor().getTableName().getNameAsString().split(":");
            if (name.length == 2) {
                namespace = name[0];
                tableName = name[1];
            }
            else {
                tableName = name[0];
            }
            regionName = region.getRegionInfo().getEncodedName();

            conf = HConfiguration.unwrapDelegate();
            rootDir = FSUtils.getRootDir(conf);
            fs = FSUtils.getCurrentFileSystem(conf);
            backupDir = new Path(rootDir, BackupRestoreConstants.BACKUP_DIR + "/data/splice/" + tableName + "/" + regionName);
            preparing = new AtomicBoolean(false);
            isCompacting = new AtomicBoolean(false);
            isSplitting = new AtomicBoolean(false);
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public Iterable<Service> getServices() {
        List<Service> services = Lists.newArrayList();
        services.add(this);
        return services;
    }


    @Override
    public void prepareBackup(
            com.google.protobuf.RpcController controller,
            BackupMessage.PrepareBackupRequest request,
            com.google.protobuf.RpcCallback<BackupMessage.PrepareBackupResponse> done) {
        try {
            preparing.set(true);
            BackupMessage.PrepareBackupResponse.Builder responseBuilder =
                    prepare(request);

            assert responseBuilder.hasReadyForBackup();
            done.run(responseBuilder.build());
        } catch (Exception e) {
            controller.setFailed(e.getMessage());
        } finally {
            preparing.set(false);
        }

    }

    public BackupMessage.PrepareBackupResponse.Builder prepare(BackupMessage.PrepareBackupRequest request) throws Exception{

        BackupMessage.PrepareBackupResponse.Builder responseBuilder = BackupMessage.PrepareBackupResponse.newBuilder();
        responseBuilder.setReadyForBackup(false);

        if (!BackupUtils.regionKeysMatch(request, region)) {
            // if the start/end key of the request does not match this region, return false to the client, because
            // region has been split. The client should retry.
            SpliceLogUtils.info(LOG, "preparing backup for table %s region %s", tableName, regionName);
            SpliceLogUtils.info(LOG, "Region keys do not match with keys in the request");
            return responseBuilder;
        }

        boolean canceled = false;

        long backupId = request.getBackupId();
        String backupJobPath = BackupUtils.getBackupPath() + "/" + backupId;
        String regionBackupPath = backupJobPath + "/" + tableName + "/" + regionName;
        if (isSplitting.get() || isCompacting.get()) {
            SpliceLogUtils.info(LOG, "table %s region %s is not ready for backup: isSplitting=%s, isCompacting=%s",
                    tableName , regionName, isSplitting.get(), isCompacting.get());

            // return false to client if the region is being split
            responseBuilder.setReadyForBackup(false);
        } else {
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "%s:%s waits for flush and compaction to complete", tableName, regionName);
            }

            // A region might have been in backup. This is unlikely to happen unless the previous response was lost
            // and the client is retrying
            if (!BackupUtils.regionIsBeingBackup(tableName, regionName, backupJobPath, regionBackupPath)) {
                // Flush memstore and Wait for flush and compaction to be done
                region.flushcache(false,false, null);
                region.waitForFlushesAndCompactions();

                canceled = BackupUtils.backupCanceled(backupId);
                if (!canceled) {
                    // Create a ZNode to indicate that the region is being copied
                    RegionInfo regionInfo = region.getRegionInfo();
                    BackupRegionStatus backupRegionStatus = BackupRegionStatus.newBuilder()
                            .setStartKey(ByteString.copyFrom(regionInfo.getStartKey()))
                            .setEndKey((ByteString.copyFrom(regionInfo.getEndKey())))
                            .setStatus(ByteString.copyFrom(HConfiguration.BACKUP_IN_PROGRESS))
                            .build();

                    boolean created = ZkUtils.recursiveSafeCreate(regionBackupPath, backupRegionStatus.toByteArray(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    if (LOG.isDebugEnabled()) {
                        if (ZkUtils.getRecoverableZooKeeper().exists(regionBackupPath, false) != null) {
                            SpliceLogUtils.debug(LOG,"created znode %s to mark backup in progress, created = %s", regionBackupPath, created);
                        }else {
                            SpliceLogUtils.warn(LOG, "failed to create znode %s, created = %s", regionBackupPath, created);
                        }
                    }

                    if (isCompacting.get() || isSplitting.get()) {

                        SpliceLogUtils.info(LOG, "table %s region %s is not ready for backup: isSplitting=%s, isCompacting=%s",
                                tableName, regionName, isSplitting.get(), isCompacting.get());
                        SpliceLogUtils.info(LOG, "delete znode %d", regionBackupPath);

                        ZkUtils.recursiveDelete(regionBackupPath);
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

//    @Override
//    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
//        try {
//            if (LOG.isDebugEnabled())
//                SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preSplit(): %s", regionName);
//
//            BackupUtils.waitForBackupToComplete(tableName, regionName);
//            isSplitting.set(true);
//            super.preSplit(e);
//        } catch (Throwable t) {
//            throw CoprocessorUtils.getIOException(t);
//        }
//    }
//
//    @Override
//    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
//        try {
//            if (LOG.isDebugEnabled())
//                SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postRollBackSplit(): %s", regionName);
//            super.postRollBackSplit(ctx);
//            isSplitting.set(false);
//        } catch (Throwable t) {
//            throw CoprocessorUtils.getIOException(t);
//        }
//
//    }
//
//    @Override
//    public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
//        try {
//            if (LOG.isDebugEnabled())
//                SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postCompleteSplit(): %s", regionName);
//            super.postCompleteSplit(ctx);
//            isSplitting.set(false);
//        } catch (Throwable t) {
//            throw CoprocessorUtils.getIOException(t);
//        }
//    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preCompact()");

            BackupUtils.waitForBackupToComplete(tableName, regionName);
            isCompacting.set(true);
            SpliceLogUtils.info(LOG, "setting isCompacting=true for %s:%s", tableName, regionName);
            return scanner == null ? DummyScanner.INSTANCE : scanner;
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request) throws IOException {
        try {
            isCompacting.set(false);
            SpliceLogUtils.info(LOG, "setting isCompacting=false for %s:%s", tableName, regionName);
            if (LOG.isDebugEnabled()) {
                String filePath =  resultFile != null?resultFile.getPath().toString():null;
                SpliceLogUtils.debug(LOG, "Compaction result file %s", filePath);
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile, FlushLifeCycleTracker tracker) throws IOException {
        try {
            // Register HFiles for incremental backup
            String filePath =  resultFile != null?resultFile.getPath().toString():null;
            SpliceLogUtils.info(LOG, "Flushed store file %s", filePath);
            if (!BackupUtils.isSpliceTable(namespace, tableName))
                return;

            BackupUtils.captureIncrementalChanges(conf, region, fs, rootDir, backupDir,
                    tableName, resultFile.getPath().getName(), preparing.get());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> familyPaths) throws IOException {

        try {
            if (!BackupUtils.isSpliceTable(namespace, tableName)||
                    !BackupUtils.shouldCaptureIncrementalChanges(fs, rootDir)) {
                return;
            }

            // Each bulkload thread has its own list of store files before loading files
            byte[] family = familyPaths.get(0).getFirst();
            Store store = region.getStore(family);
            storeFiles.set(store.getStorefiles());
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @SuppressFBWarnings(value="UL_UNRELEASED_LOCK_EXCEPTION_PATH")
    @Override
    public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx, List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths) throws IOException {

        try {
            if (!BackupUtils.isSpliceTable(namespace, tableName) ||
                    !BackupUtils.shouldCaptureIncrementalChanges(fs, rootDir))
                return;

            try {
                // Only one bulkload thread can register HFiles for incremental backup at a time. It registers newly
                // bulk loaded HFile between the time period when preBulkLoadHFile and postBulkLoadHFile are invoked.
                // There might be multiple bulk load running for the same region, so it could register HFiles loaded
                // by other bulkload threads. It is fine if multiple bulk load see the same incremental changes and
                // try to register them.

                bulkLoadLock.lock();
                byte[] family = stagingFamilyPaths.get(0).getFirst();
                Store store = region.getStore(family);
                Collection<? extends StoreFile> postBulkLoadStoreFiles = store.getStorefiles();
                for (StoreFile storeFile : postBulkLoadStoreFiles) {
                    byte[] val = ((HStoreFile)storeFile).getMetadataValue(HStoreFile.BULKLOAD_TASK_KEY);
                    if (val != null && Bytes.compareTo(val, HBaseConfiguration.BULKLOAD_TASK_KEY) == 0
                            && !storeFiles.get().contains(storeFile)) {
                        BackupUtils.registerHFile(conf, fs, backupDir, region, storeFile.getPath().getName());
                    }
                }
            }
            finally {
                storeFiles.remove();
                bulkLoadLock.unlock();
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "released bulk load lock for region %s", regionName);
                }
            }
        }
        catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return optionalRegionObserver;
    }
}
