package com.splicemachine.hbase;

import com.google.protobuf.Service;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

/**
 * Created by jyuan on 2/18/16.
 */
public class BackupEndpointObserver extends BackupBaseRegionObserver implements CoprocessorService,Coprocessor {
    private static final Logger LOG=Logger.getLogger(BackupEndpointObserver.class);

    private boolean isSplitting;
    private HRegion region;
    private String tableName;
    private String regionName;
    private String path;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableName = region.getTableDesc().getNameAsString();
        regionName = region.getRegionNameAsString();
        path = HConfiguration.DEFAULT_BACKUP_PATH + "/" + tableName + "/" + regionName;
    }

    @Override
    public Service getService(){
        return this;
    }


    @Override
    public void prepareBackup(
            com.google.protobuf.RpcController controller,
            com.splicemachine.coprocessor.SpliceMessage.PrepareBackupRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.coprocessor.SpliceMessage.PrepareBackupResponse> done) {

        try {
            SpliceMessage.PrepareBackupResponse.Builder responseBuilder = SpliceMessage.PrepareBackupResponse.newBuilder();
            if (isSplitting) {
                // return an error if the region is being split
                responseBuilder.setReadyForBackup(false);
            } else {
                region.flushcache();
                // Create a ZNode to indicate that region is being copied
                ZkUtils.recursiveSafeCreate(path, HConfiguration.BACKUP_IN_PROGRESS, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                // check again if the region is being split. If so, return an error
                if (isSplitting) {
                    responseBuilder.setReadyForBackup(false);
                    //delete the ZNode
                    ZkUtils.recursiveDelete(path);
                } else {
                    //wait for all compaction and flush to complete
                    region.waitForFlushesAndCompactions();
                    responseBuilder.setReadyForBackup(true);
                }
            }
            done.run(responseBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "BackupEndpointObserver.preSplit()");

        waitForBackupToComplete();
        isSplitting = true;
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        isSplitting = false;
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
        waitForBackupToComplete();
        return super.preCompact(e, store, scanner, scanType);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        waitForBackupToComplete();
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile) throws IOException {
    }

    private void waitForBackupToComplete() throws IOException{
        while (regionIsBeingBackup()) {
            try {
                Thread.sleep(100);
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
                isBackup = false;
            }
            else {
                byte[] status = ZkUtils.getData(path);
                if (Bytes.compareTo(status, HConfiguration.BACKUP_IN_PROGRESS) == 0) {
                    isBackup = true;
                }
                else if (Bytes.compareTo(status, HConfiguration.BACKUP_DONE) == 0) {
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
}
