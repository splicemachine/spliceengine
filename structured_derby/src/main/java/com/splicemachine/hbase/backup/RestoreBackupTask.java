package com.splicemachine.hbase.backup;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 *
 */
public class RestoreBackupTask extends ZkTask {
    private static final long serialVersionUID = 5l;
    private BackupItem backupItem;

    public RestoreBackupTask() {
    }

    public RestoreBackupTask(BackupItem backupItem, String jobId) {
        super(jobId, OperationJob.operationTaskPriority);
        this.backupItem = backupItem;
    }

    @Override
    protected String getTaskType() {
        return "restoreBackupTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(backupItem); // TODO Needs to be replaced with protobuf
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        backupItem = (BackupItem) in.readObject(); // TODO Needs to be replaced with protobuf
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        return new RestoreBackupTask(backupItem, jobId);
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
            region.bulkLoadHFiles(famPaths);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Couldn't recover region " + region, e);
            throw new ExecutionException("Failed recovery of region " + region, e);
        }
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
}

