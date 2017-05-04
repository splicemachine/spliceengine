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
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by jyuan on 2/18/16.
 */
public class BackupEndpointObserver extends BackupBaseRegionObserver implements CoprocessorService,Coprocessor {
    private static final Logger LOG=Logger.getLogger(BackupEndpointObserver.class);

    private volatile boolean isSplitting;
    private volatile boolean isFlushing;
    private volatile boolean isCompacting;
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
        region = (HRegion) ((RegionCoprocessorEnvironment) e).getRegion();
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
            SpliceMessage.PrepareBackupResponse.Builder responseBuilder =
                    BackupUtils.prepare(request, region, isSplitting, isCompacting, isFlushing, tableName, regionName, path);
            preparing = false;
            assert responseBuilder.hasReadyForBackup();
            done.run(responseBuilder.build());
        } catch (Exception e) {
            controller.setFailed(e.getMessage());
        }

    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preSplit()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isSplitting = true;
        }
        super.preSplit(e);
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preSplit()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isSplitting = true;
        }
        super.preSplit(c, splitRow);
    }

    @Override
    public void postRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postRollBackSplit()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            isSplitting = false;
        }
        super.postRollBackSplit(ctx);
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> regionCoprocessorEnvironmentObserverContext, Region region, Region region2) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postSplit()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            isSplitting = false;
        }
    }
    
    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.postSplit()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            isSplitting = false;
        }
        super.postSplit(e, l, r);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preCompactSelection()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isCompacting = true;
        }
        super.preCompactSelection(c, store, candidates);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preCompact()");
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isCompacting = true;
        }
        return super.preCompact(e, store, scanner, scanType);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
        super.postCompact(e, store, resultFile);
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            isCompacting = false;
        }
        super.postCompact(e, store, resultFile);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preFlush(): %s.%s", tableName, regionName);
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isFlushing = true;
        }
        super.preFlush(e);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preFlush(): %s.%s", tableName, regionName);
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            BackupUtils.waitForBackupToComplete(tableName, regionName, path);
            isFlushing = true;
        }
        return super.preFlush(e,store,scanner);
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
        // Register HFiles for incremental backup
        SpliceLogUtils.info(LOG, "Flushing region %s.%s: %s", tableName, regionName, resultFile.getPath().toString());
        try {
            if (BackupUtils.isSpliceTable(namespace, tableName)) {
                BackupUtils.captureIncrementalChanges(conf, region, path, fs, rootDir, backupDir,
                        tableName, resultFile.getPath().getName(), preparing);
                isFlushing = false;
            }
            super.postFlush(e, store, resultFile);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        SpliceLogUtils.info(LOG, "BackupEndpointObserver.postFlush() %s.%s", tableName, regionName);
        if (BackupUtils.isSpliceTable(namespace, tableName)) {
            isFlushing = false;
        }
        super.postFlush(e);
    }
}
