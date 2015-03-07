package com.splicemachine.hbase.backup;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.HashMap;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

/**
 * Created by jyuan on 3/4/15.
 */
public class CreateIncrementalBackupTask extends ZkTask {

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

    public CreateIncrementalBackupTask() { init();}

    public CreateIncrementalBackupTask(BackupItem backupItem,
                                       String jobId,
                                       String backupFileSystem,
                                       String snapshotNameame,
                                       String lastSnapshotName) {
        super(jobId, OperationJob.operationTaskPriority);
        this.backupItem = backupItem;
        this.backupFileSystem = backupFileSystem;
        this.snapshotName = snapshotNameame;
        this.lastSnapshotName = lastSnapshotName;
        init();
    }

    private void init() {
        excludeFileSet = new HashSet<>();
        includeFileSet = new HashSet<>();
    }
    @Override
    protected String getTaskType() {
        return "createIncrementalBackupTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
        out.writeUTF(backupFileSystem);
        out.writeUTF(snapshotName);
        out.writeUTF(lastSnapshotName);
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
        backupFileSystem = in.readUTF();
        snapshotName = in.readUTF();
        lastSnapshotName = in.readUTF();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        return new CreateBackupTask(backupItem, jobId, backupFileSystem);
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing incremental backup on region %s", region.toString()));

        incrementalBackup();
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(CreateBackupTask.class);
    }

    private void incrementalBackup() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing incremental backup on region %s", region.toString()));

        tableName = region.getTableDesc().getNameAsString();
        encodedRegionName = region.getRegionInfo().getEncodedName();
        int count = 0;
        try{
            populateExcludeFileSet();
            populateIncludeFileSet();
            List<Path> paths = getIncrementalChanges();
            FileSystem fs = region.getFilesystem();

            BackupUtils.derbyFactory.writeRegioninfoOnFilesystem(region.getRegionInfo(),
                    new Path(backupFileSystem), fs, SpliceConstants.config);

            if (paths != null && paths.size() > 0) {
                LOG.error(region.getRegionNameAsString());
                for (Path p : paths) {
                    String[] s = p.toString().split("/");
                    int n = s.length;
                    String fileName = s[n - 1];
                    String familyName = s[n - 2];
                    String regionName = s[n - 3];
                    Path destPath = new Path(backupFileSystem + "/" + regionName + "/" + familyName + "/" + fileName);
                    FileUtil.copy(fs, p, fs, destPath, false, SpliceConstants.config);
                    count++;
                }
            }

            count += copyArchivedHFiles();
            if (count == 0) {
                fs.delete(new Path(backupFileSystem + "/" + encodedRegionName), true);
                Path path = new Path(backupFileSystem);
                FileStatus[] status = fs.listStatus(path);
                if (status.length == 0) {
                    fs.delete(path, true);
                }
            }
        }
        catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private int copyArchivedHFiles() throws IOException{

        int count = 0;
        FileSystem fileSystem = region.getFilesystem();
        Path regionArchivedDir = HFileArchiveUtil.getRegionArchiveDir(rootDir,
                region.getTableDesc().getTableName(), encodedRegionName);
        if (!fileSystem.exists(regionArchivedDir) || includeFileSet.size() == 0) {
            return 0;
        }
        FileStatus[] status = fileSystem.listStatus(regionArchivedDir);
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
                Path destPath = new Path(backupFileSystem + "/" + encodedRegionName + "/" + family + "/" + fileName);
                if(includeFileSet.contains(fileName)) {
                    FileUtil.copy(fileSystem, srcPath, fileSystem, destPath, false, SpliceConstants.config);
                    ++count;
                }
            }
        }
        BackupUtils.deleteFileSet(tableName, encodedRegionName, "%", true);
        return count;
    }

    private List<Path> getIncrementalChanges() throws ExecutionException{

        List<Path> hFiles = null;
        List<Path> paths = null;
        List<Path> lastPaths = null;

        try {
            Configuration conf = SpliceConstants.config;
            FileSystem fs = FileSystem.get(URI.create(backupFileSystem), conf);
            rootDir = FSUtils.getRootDir(conf);
            Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
            paths = SnapshotUtils.getSnapshotFilesForRegion(region ,conf, fs, snapshotDir);
            if (lastSnapshotName != null) {
                Path lastSnapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(lastSnapshotName, rootDir);
                lastPaths = SnapshotUtils.getSnapshotFilesForRegion(region, conf, fs, lastSnapshotDir);
            }
        } catch (IOException e) {
            throw new ExecutionException(e);
        }

        if (lastPaths == null)
            return paths;
        else {

            // Hash files from the latest snapshot, ignore files that should be excluded
            HashMap<String, Path> pathMap = new HashMap<>();
            for(Path p : paths) {
                String name = p.getName();
                if (!excludeFileSet.contains(name)) {
                    pathMap.put(name, p);
                }
            }

            // remove an HFile if it also appears in a previous snapshot
            for(Path p : lastPaths) {
                String name = p.getName();
                if (pathMap.containsKey(name)) {
                    pathMap.remove(name);
                }
            }

            Collection<Path> r = pathMap.values();
            hFiles = new ArrayList<>(r);
        }
        return hFiles;
    }

    private void populateExcludeFileSet() throws SQLException{
        ResultSet rs = BackupUtils.queryFileSet(tableName, encodedRegionName, false);
        if (rs != null) {
            while (rs.next()) {
                excludeFileSet.add(rs.getString(1));
            }
        }
    }

    private void populateIncludeFileSet() throws SQLException{
        ResultSet rs = BackupUtils.queryFileSet(tableName, encodedRegionName, true);
        if (rs != null) {
            while (rs.next()) {
                includeFileSet.add(rs.getString(1));
            }
        }
    }
}
