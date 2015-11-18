package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 *
 *
 */
public class RestoreBackupTask {
    private static Logger LOG = Logger.getLogger(RestoreBackupTask.class);
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final long serialVersionUID = 5l;
    HRegion region = null;
    private BackupItem backupItem;

    public RestoreBackupTask() {
    }

    public RestoreBackupTask(BackupItem backupItem, String jobId) {
        this.backupItem = backupItem;
    }


    public void doExecute() throws ExecutionException, InterruptedException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, String.format("executing %S backup on region %s", backupItem.getBackup().isIncrementalBackup() ? "incremental" : "full", region.toString()));
        List<Pair<byte[], String>> copyPaths = null;
        FileSystem fs = region.getFilesystem();
        try {
            BackupItem.RegionInfo regionInfo = getRegionInfo();
            if (regionInfo == null) {
                return;
            }
            List<Pair<byte[], String>> famPaths = regionInfo.getFamPaths();
            copyPaths = copyStoreFiles(famPaths);
            derbyFactory.bulkLoadHFiles(region, copyPaths);
            if (copyPaths != null) {
                for (Pair<byte[], String> p : copyPaths) {
                    String path = p.getSecond();
                    fs.delete(new Path(path), true);
                }
            }
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
            if (comparator.compare(ri.getHRegionInfo().getEndKey(), region.getRegionInfo().getEndKey()) == 0) {
                return ri;
            }
        }
        SpliceLogUtils.warn(LOG, "Couldn't find matching backup data for region " + region);
        return null;
    }

    static Path getRandomFilename(final FileSystem fs,
                                  final Path dir)
            throws IOException {
        return new Path(dir, UUID.randomUUID().toString().replaceAll("-", ""));
    }
}

