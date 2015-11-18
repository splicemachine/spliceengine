package com.splicemachine.hbase.backup;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.io.IOUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.HashMap;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by jyuan on 3/4/15.
 */
public class CreateIncrementalBackupTask {

    private static Logger LOG = Logger.getLogger(CreateIncrementalBackupTask.class);

    private static final long serialVersionUID = 5l;
    private BackupItem backupItem;
    private String backupFileSystem;
    private String snapshotName;
    private String lastSnapshotName;
    private Set<String> excludeFileSet;
    private Set<String> includeFileSet;
    private String tableName;
    private String encodedRegionName;
    private Path rootDir;
    SnapshotUtils snapshotUtils = SnapshotUtilsFactory.snapshotUtils;

    public CreateIncrementalBackupTask() { init();}

    public CreateIncrementalBackupTask(BackupItem backupItem,
                                       String jobId,
                                       String backupFileSystem,
                                       String snapshotName,
                                       String lastSnapshotName) {
        this.backupItem = backupItem;
        this.backupFileSystem = backupFileSystem;
        this.snapshotName = snapshotName;
        this.lastSnapshotName = lastSnapshotName;
        init();
    }

    private void init() {
        excludeFileSet = new HashSet<>();
        includeFileSet = new HashSet<>();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
        out.writeUTF(backupFileSystem);
        out.writeUTF(snapshotName);
        out.writeUTF(lastSnapshotName);
        init();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
        backupFileSystem = in.readUTF();
        snapshotName = in.readUTF();
        lastSnapshotName = in.readUTF();
    }

    public void doExecute() throws ExecutionException, InterruptedException {
        incrementalBackup();
    }

    /**
     * This method copies incremental changes of a region. Incremental changes includes the following
     * 1) HFiles that are in the latest snapshot, but not in previous snapshot, or in BACKUP.BACKUP_FILESET with
     *    include=false
     * 2) HFiles that only contains incremental changes but not in the latest snapshot. These HFiles are moved to
     *    HBase archived directory after compaction. BACKUP.BACKUP_FILESET records each HFile with include=true.
     * If there are no incremental changes for this region, do not write anything(region directory,
     * region information, etc) to file system.
     */
    private void incrementalBackup() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing incremental backup on region %s", "TODO"));
        // JL TODO
        /*
        boolean throttleEnabled =
                getConfiguration().getBoolean(Backup.CONF_IOTHROTTLE, false);
        tableName = region.getTableDesc().getNameAsString();
        encodedRegionName = region.getRegionInfo().getEncodedName();
        int count = 0;
        Configuration conf = SpliceConstants.config;
        try{
            populateFileSet();
            List<Path> paths = getIncrementalChanges();
            FileSystem regionfs = region.getFilesystem();
            FileSystem backupfs = FileSystem.get(URI.create(backupFileSystem), conf);


            // Write region information
            BackupUtils.derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(),
                    new Path(backupFileSystem), backupfs, conf);

            // Copy HFiles that only in the latest snapshot and not in BACKUP.BACKUP_FILESET with include=false
            if (paths != null && paths.size() > 0) {
                for (Path p : paths) {
                    Path from = p;

                    String[] s = p.toString().split("/");
                    int n = s.length;
                    String fileName = s[n - 1];
                    String familyName = s[n - 2];
                    String regionName = s[n - 3];
                    if (BackupUtils.isReference(p.getName())) {
                        // materialize the reference file first
                        HFileLink fileLink = snapshotUtils.createHFileLink(conf,
                                region.getTableDesc().getTableName(), encodedRegionName, familyName, p.getName());

                        from = snapshotUtils.materializeRefFile(conf, regionfs, fileLink, region);
                        fileName = from.getName();
                    }

                    Path destPath = new Path(backupFileSystem + "/" + regionName + "/" + familyName + "/" + fileName);
                    if(throttleEnabled){
                        IOUtils.copyFileWithThrottling(regionfs, from, backupfs, destPath, false, SpliceConstants.config);
                    } else{
                        FileUtil.copy(regionfs, from, backupfs, destPath, false, SpliceConstants.config);
                    }
                    count++;
                }
            }

            count += copyArchivedHFiles();
            if (count == 0 && lastSnapshotName != null) {
                // The directory becomes empty if the table has no incremental changes, and the table is not
                // a new empty table, and there were no region split for the table. No need to keep the directory
                // in this case.
                backupfs.delete(new Path(backupFileSystem + "/" + encodedRegionName), true);
            }
        }
        catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
        */
    }

    private Configuration getConfiguration() {
        return SpliceConstants.config;
    }

    /**
     * For each HFile in archived directory, check whether it should be included in incremental backup.
     * Delete all entries for this region in BACKUP.BACKUP_FILESET. BACKUP.BACKUP_FILESET should only keeps track of
     * HFile changes between two consecutive incremental backups.
     */
    private int copyArchivedHFiles() throws IOException, StandardException{
        /*
        if (includeFileSet.size() == 0) {
            return 0;
        }

        int count = 0;
        FileSystem regionfs = region.getFilesystem();
        Map<byte[], Store> stores = HBasePlatformUtils.getStores(region);
        Configuration conf = SpliceConstants.config;
        FileSystem backupfs = FileSystem.get(URI.create(backupFileSystem), conf);

        for (byte[] family : stores.keySet()) {
            HRegionInfo regionInfo = region.getRegionInfo();
            DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
            Path tableDir = derbyFactory.getTableDir(region);
            Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, regionInfo, tableDir, family);
            if (!regionfs.exists(storeArchiveDir)) {
                return 0;
            }
            String familyName = storeArchiveDir.getName();
            FileStatus[] status = regionfs.listStatus(storeArchiveDir);
            for (FileStatus stat : status) {
                Path srcPath = stat.getPath();
                String fileName = srcPath.getName();
                Path destPath = new Path(backupFileSystem + "/" + encodedRegionName + "/" + familyName + "/" + fileName);
                if (includeFileSet.contains(fileName)) {
                    if (BackupUtils.isReference(fileName)) {
                        // materialize the reference file first
                        HFileLink fileLink = snapshotUtils.createHFileLink(conf,
                                region.getTableDesc().getTableName(), encodedRegionName, familyName, fileName);

                        srcPath = snapshotUtils.materializeRefFile(conf, regionfs, fileLink, region);
                        fileName = srcPath.getName();
                    }
                    FileUtil.copy(regionfs, srcPath, backupfs, destPath, false, conf);
                    ++count;
                    BackupUtils.deleteFileSet(tableName, encodedRegionName, fileName, true);
                }
            }
        }
        return count;
        */
        return -1;
    }

    /*
     * This method compares the latest snapshot and a previous snapshot, returns files that are only in the latest
     * snapshot AND not in BACKUP.BACKUP_FILESET with include column being false
     */
    private List<Path> getIncrementalChanges() throws ExecutionException{
        /*

        List<Path> hFiles = null;
        List<Object> paths = null;
        List<Object> lastPaths = null;

        try {
            Configuration conf = SpliceConstants.config;


            rootDir = FSUtils.getRootDir(conf);
            FileSystem regionfs = rootDir.getFileSystem(conf);

            // Get files that are in the latest snapshot
            paths = snapshotUtils.getSnapshotFilesForRegion(region, conf, regionfs, snapshotName, false);
            if (lastSnapshotName != null) {
                // Get files that are in a previous snapshot
                lastPaths = snapshotUtils.getSnapshotFilesForRegion(region, conf, regionfs, lastSnapshotName, false);
            }

            // Hash files from the latest snapshot, ignore files that should be excluded
            HashMap<String, Path> pathMap = new HashMap<>();
            for(Object o : paths) {
                Path p = ((HFileLink) o).getAvailablePath(regionfs);
                String name = p.getName();
                if (!excludeFileSet.contains(name)) {
                    pathMap.put(name, p);
                }
            }

            if (lastPaths != null && lastPaths.size() > 0) {
                // remove an HFile if it also appears in a previous snapshot
                for (Object o : lastPaths) {
                    Path p = ((HFileLink) o).getAvailablePath(regionfs);
                    String name = p.getName();
                    if (pathMap.containsKey(name)) {
                        pathMap.remove(name);
                    }
                }
            }
            Collection<Path> r = pathMap.values();
            hFiles = new ArrayList<>(r);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }

        return hFiles;
        */
        return null;
    }

    private void populateFileSet() throws StandardException {
        BackupFileSetReporter backupFileSetReporter = BackupUtils.scanFileSetForRegion(tableName, encodedRegionName);
        BackupFileSet backupFileSet = null;
        while ((backupFileSet = backupFileSetReporter.next()) != null) {
            if (backupFileSet.shouldInclude()) {
                includeFileSet.add(backupFileSet.getFileName());
            }
            else {
                excludeFileSet.add(backupFileSet.getFileName());
            }
        }

        backupFileSetReporter.closeScanner();
    }
}