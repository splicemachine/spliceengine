package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 *
 *
 */
public class RestoreBackupTask extends ZkTask {
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final long serialVersionUID = 5l;
    private BackupItem backupItem;
    private List<Long> backupIds;

    public RestoreBackupTask() {
    }

    public RestoreBackupTask(BackupItem backupItem, List<Long> backupIds, String jobId) {
        super(jobId, OperationJob.operationTaskPriority);
        this.backupItem = backupItem;
        this.backupIds = backupIds;
    }

    @Override
    protected String getTaskType() {
        return "restoreBackupTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
        int size = backupIds.size();
        out.writeInt(size);
        for (int i = 0; i < size; ++i) {
            out.writeLong(backupIds.get(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
        int size = in.readInt();
        if (size > 0) {
            backupIds = new ArrayList<Long>(size);
            for (int i = 0; i < size; ++i) {
                backupIds.add(in.readLong());
            }
        }
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        return new RestoreBackupTask(backupItem, backupIds, jobId);
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing %S backup on region %s", backupItem.getBackup().isIncrementalBackup() ? "incremental" : "full", region.toString()));
        try {
            BackupItem.RegionInfo regionInfo = getRegionInfo();
            if (regionInfo == null) {
                return;
            }
            List<Pair<byte[], String>> famPaths = regionInfo.getFamPaths();
            List<Pair<byte[], String>> copyPaths = copyStoreFiles(famPaths);
            derbyFactory.bulkLoadHFiles(region, copyPaths);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Couldn't recover region " + region, e);
            throw new ExecutionException("Failed recovery of region " + region, e);
        }
    }

    // Copy store files to preserve them since HBase's bulkload moves the files directly in place to avoid extra copies
    private List<Pair<byte[], String>> copyStoreFiles(List<Pair<byte[], String>> famPaths) throws IOException {
        List<Pair<byte[], String>> copy = new ArrayList<Pair<byte[], String>>(famPaths.size());
        FileSystem fs = region.getFilesystem();
        for (Pair<byte[], String> pair : famPaths) {
            Path srcPath = new Path(pair.getSecond());
            FileSystem srcFs = srcPath.getFileSystem(SpliceConstants.config);
            Path localDir = derbyFactory.getRegionDir(region);
            Path tmpPath = getRandomFilename(fs, localDir);
            FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, SpliceConstants.config);
            copy.add(new Pair<byte[], String>(pair.getFirst(), tmpPath.toString()));
        }
        return copy;
    }

    private BackupItem.RegionInfo getRegionInfo() {
        Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();
        for (BackupItem.RegionInfo ri : backupItem.getRegionInfoList()) {
            if (comparator.compare(ri.getHRegionInfo().getEndKey(), region.getEndKey()) == 0) {
                return ri;
            }
        }
        SpliceLogUtils.warn(LOG, "Couldn't find matching backup data for region " + region);
        return null;
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(RestoreBackupTask.class);
    }

    static Path getRandomFilename(final FileSystem fs,
                                  final Path dir)
            throws IOException {
        return new Path(dir, UUID.randomUUID().toString().replaceAll("-", ""));
    }
}

