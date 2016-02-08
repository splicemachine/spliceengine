package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.backup.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.CompactionObserver;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import com.splicemachine.derby.hbase.SplitObserver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class BackupObserver implements CompactionObserver,SplitObserver{
    private static final Logger LOG_COMPACT = Logger.getLogger(SpliceIndexObserver.class.getName() + ".Compaction");
    private static final Logger LOG_SPLIT = Logger.getLogger(SpliceIndexObserver.class.getName() + ".Split");
    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,InternalScanner scanner,ScanType scanType,CompactionRequest request) throws IOException{
        try{
            waitForBackupToComplete();
        }catch(InterruptedException e1){
            Thread.currentThread().interrupt();
        }
        return scanner;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,Store store,StoreFile resultFile,CompactionRequest request) throws IOException{
        recordCompactedFilesForBackup(e, request, resultFile);
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,byte[] splitRow) throws IOException{
        try{
            waitForBackupToComplete();
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e,HRegion l,HRegion r) throws IOException{

//        recordRegionSplitForBackup(e, l, r);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private void waitForBackupToComplete() throws InterruptedException{
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        throw new UnsupportedOperationException("Implement using Watches and good concurrency!");
//        while (zooKeeper.exists(SpliceConstants.DEFAULT_BACKUP_PATH, false) != null) {
//            Thread.sleep(1000);
//        }
    }

    private void recordRegionSplitForBackup(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException{
        try {

            // Nothing needs to be done if there was never a succeeded backup
            boolean exists = BackupUtils.existBackupWithStatus(Backup.BackupStatus.S.toString(),0);
            if (!exists)
                return;

//            if (LOG.isTraceEnabled()) {
//                SpliceLogUtils.trace(LOG_SPLIT, "postSplit: parent region " +
//                        e.getEnvironment().getRegion().getRegionInfo().getEncodedName());
//                SpliceLogUtils.trace(LOG_SPLIT, "postSplit: l region " + l.getRegionInfo().getEncodedName());
//                SpliceLogUtils.trace(LOG_SPLIT, "postSplit: r region " + r.getRegionInfo().getEncodedName());
//            }

            Configuration conf =HConfiguration.INSTANCE.unwrapDelegate();
            SnapshotUtils snapshotUtils = SnapshotUtilsFactory.snapshotUtils;
            HRegion region =e.getEnvironment().getRegion();
            FileSystem fs = region.getFilesystem();

            String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
            String encodedRegionName = e.getEnvironment().getRegion().getRegionInfo().getEncodedName();

            Set<String> pathSet = new HashSet<>();
            // Get HFiles from last snapshot
            String snapshotName = BackupUtils.getLastSnapshotName(tableName);
            if (snapshotName != null) {
                List<Object> pathList = snapshotUtils.getSnapshotFilesForRegion(region, conf, fs, snapshotName, false);

                for (Object o : pathList) {
                    Path p = ((HFileLink) o).getAvailablePath(fs);
                    String name = p.getName();
                    pathSet.add(name);
                }
            }
            HashMap<byte[], Collection<StoreFile>> lStoreFileInfoMap = BackupUtils.getStoreFiles(l);
            HashMap<byte[], Collection<StoreFile>> rStoreFileInfoMap = BackupUtils.getStoreFiles(r);

            updateFileSet(tableName, encodedRegionName, pathSet, lStoreFileInfoMap, l);
            updateFileSet(tableName, encodedRegionName, pathSet, rStoreFileInfoMap, r);
        }catch (Exception ex) {
            SpliceLogUtils.warn(LOG_SPLIT,"Error in recordRegionSplitForBackup: %s",ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void updateFileSet(String tableName,
                               String parentRegionName,
                               Set<String> pathSet,
                               HashMap<byte[], Collection<StoreFile>> storeFileInfoMap,
                               HRegion r) throws StandardException{

        if (LOG_SPLIT.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG_SPLIT, "updateFileSet: region = " + r.getRegionInfo().getEncodedName());
        }
        String childRegionName = r.getRegionInfo().getEncodedName();
        for(Collection<StoreFile> storeFiles : storeFileInfoMap.values()) {
            for (StoreFile storeFile : storeFiles) {
                String referenceFileName = storeFile.getPath().getName();
                String[] s = referenceFileName.split("\\.");
                if (s.length == 1)
                    continue;
                String fileName = s[0];
                String regionName = s[1];
                if (LOG_SPLIT.isTraceEnabled()) {
                    SpliceLogUtils.trace(LOG_SPLIT, "updateFileSet: referenceFileName = " + referenceFileName);
                    SpliceLogUtils.trace(LOG_SPLIT, "updateFileSet: referenced file = " + fileName);
                }
                if (pathSet.contains(fileName)) {
                    // If referenced file appears in the last snapshot
                    // then file in child region should not be included in next incremental backup
                    BackupUtils.insertFileSet(tableName, childRegionName, referenceFileName, false);
                }
                else {
                    BackupFileSet fileSet = BackupUtils.getFileSet(tableName, regionName, fileName);
                    if (fileSet != null) {
                        // If there is an entry in SYS.BackupFileSet for the referenceD file, an entry should
                        // be created for reference file
                        BackupUtils.insertFileSet(fileSet.getTableName(), childRegionName,
                                referenceFileName, fileSet.shouldInclude());
                    }
                }
            }
        }
    }

    private void recordCompactedFilesForBackup(ObserverContext<RegionCoprocessorEnvironment> e,
                                               CompactionRequest request, StoreFile resultFile) throws IOException{

        // During compaction, an HFile may need to be registered to Backup.FileSet for incremental backup
        // Check HFiles in compaction request, and classify them into 2 categories
        // 1. HFile that contains new data only. a HFile does not appear in the last snapshot, and was not a result of
        //    a recent(after last snapshot) compaction. For latter case, Backup.FileSet contains a row for the HFile
        //    and indicate it should be excluded.
        // 2. HFile that contains old data. A HFile either appears in the last snapshot, or was a result of a recent
        //    compaction.
        //
        // If the set of HFiles are all in category 1, there is no need to keep track of HFiles. The resultant HFile
        // contains new data only, and the next incremental backup will copy it.
        // If the set of HFiles are all in category 2, insert an entry into BACKUP.FileSet for the resultant HFile, and
        // indicate it should be excluded for next incremental backup.
        // If the set of HFile are in both category 1 and 2. Then for each HFile in category 1, insert an entry and
        // indicate it should be included for next incremental backup. Insert an entry for resultant HFile to indicate
        // it should be excluded for next incremental backup.
        try {
            // Nothing needs to be done if there was never a succeeded backup
            boolean exists = BackupUtils.existBackupWithStatus(Backup.BackupStatus.S.toString(), 0);
            if (!exists)
                return;

            if (LOG_COMPACT.isTraceEnabled()) {
                LOG_COMPACT.trace("Files to compact:");
                for (StoreFile storeFile : request.getFiles()) {
                    LOG_COMPACT.trace(storeFile.getPath().toString());
                }
                LOG_COMPACT.trace("resultFile = :" + resultFile.getPath().toString());
            }
            HRegion region =e.getEnvironment().getRegion();
            FileSystem fs = region.getFilesystem();
            String encodedRegionName = e.getEnvironment().getRegion().getRegionInfo().getEncodedName();
            Set<String> pathSet = new HashSet<>();
            SnapshotUtils snapshotUtils = SnapshotUtilsFactory.snapshotUtils;

            // Get HFiles from last snapshot
            String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
            String snapshotName = BackupUtils.getLastSnapshotName(tableName);
            if (snapshotName != null) {
//                SpliceLogUtils.trace(LOG, "Retrived snapshot %s.", snapshotName);
                Configuration conf = HConfiguration.INSTANCE.unwrapDelegate();
                List<Object> pathList = snapshotUtils.getSnapshotFilesForRegion(region, conf, fs, snapshotName, false);

                for (Object obj : pathList) {
                    org.apache.hadoop.fs.Path path = ((HFileLink) obj).getAvailablePath(fs);
                    pathSet.add(path.getName());
                }
            }

            // Classify HFiles in compact request into old HFile and new HFile
            List<StoreFile> oldHFile = new ArrayList<>();
            List<StoreFile> newHFile = new ArrayList<>();
            Collection<StoreFile> storeFiles = request.getFiles();
            for(StoreFile storeFile : storeFiles) {
                Path p = storeFile.getPath();
                String name = p.getName();
                if(pathSet.contains(name)) {
                    // This HFile appears in the last snapshot, so it was copied to a backup file system
                    oldHFile.add(storeFile);
                }
                else if (BackupUtils.shouldExclude(tableName, encodedRegionName, name) ||
                        BackupUtils.shouldExcludeReferencedFile(tableName, name, fs)) {
                    // This file appears in BACKUP.BACKUP_FILESET with include=false.
                    oldHFile.add(storeFile);
                }
                else {
                    newHFile.add(storeFile);
                }
            }

            if (newHFile.size() == 0 && oldHFile.size() > 0) {
                BackupUtils.insertFileSet(tableName, encodedRegionName, resultFile.getPath().getName(), false);
                for(StoreFile storeFile : oldHFile) {
                    // These HFile will be removed to archived directory and purged.
                    BackupUtils.deleteFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), false);
                }
            }
            else if (newHFile.size() > 0 && oldHFile.size() > 0) {
                for (StoreFile storeFile : newHFile) {
                    BackupUtils.insertFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), true);
                }
                BackupUtils.insertFileSet(tableName, encodedRegionName, resultFile.getPath().getName(), false);
                for (StoreFile storeFile:oldHFile) {
                    // These HFile will be removed to archived directory and purged.
                    BackupUtils.deleteFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), false);
                }
            }
        }
        catch (Exception ex) {
//            SpliceLogUtils.warn(LOG, "%s", ex.getMessage());
            ex.printStackTrace();
        }
    }
}
