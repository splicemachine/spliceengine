package com.splicemachine.hbase.backup;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.log4j.Logger;

/**
 * Created by jyuan on 1/29/15.
 */
public class BackupHFileCleaner extends BaseHFileCleanerDelegate {

    private static final Logger LOG = Logger.getLogger(BackupHFileCleaner.class);

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }

    @Override
    public synchronized boolean isFileDeletable(FileStatus fStat) {
        String path = fStat.getPath().toString();
        String[] s = path.split("/");
        int n = s.length;
        String tableName = s[n-4];
        String encodedRegionName = s[n-3];
        String fileName = s[n-1];

        return (!shouldRetainForBackup(tableName, encodedRegionName, fileName));
    }


    private boolean shouldRetainForBackup(String backupItem, String encodedRegionName, String fileName) {
        try {
            BackupFileSet backupFileSet = BackupUtils.getFileSet(backupItem, encodedRegionName, fileName);
            if (backupFileSet != null) {
                if (backupFileSet.shouldInclude()) {
                    if (LOG.isTraceEnabled()) {
                        SpliceLogUtils.trace(LOG,
                                "File %s cannot be removed from archived directory " +
                                        "because it is needed by incremental backup.", fileName
                        );
                    }
                    return true;
                } else {
                    if (LOG.isTraceEnabled()) {
                        SpliceLogUtils.trace(LOG,
                                "File %s can be removed from archived directory " +
                                        "because it's not needed by incremental backup", fileName
                        );
                        BackupUtils.deleteFileSet(backupItem, encodedRegionName, fileName, false);
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.warn(LOG, "Failed to query Backup.Backup_FileSet table.");
            }
            return false;
        }
        return false;
    }
}
