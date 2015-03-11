package com.splicemachine.hbase.backup;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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


    private boolean shouldRetainForBackup(String backupItem, String encodedRegionName, String fileName){
        Connection connection = null;
        String sqlText = "select include from %s.%s where backup_item=? and region_name=? and file_name=?";
        try{
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(sqlText, Backup.DEFAULT_SCHEMA, BackupUtils.BACKUP_FILESET_TABLE));
            ps.setString(1, backupItem);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                boolean include = rs.getBoolean(1);
                if (LOG.isTraceEnabled()) {
                    SpliceLogUtils.trace(LOG,
                            "File %s cannot be removed from archived directory " +
                            "because it is needed by incremental backup.", fileName);
                }
                if (include) {
                    return true;
                }
                else {
                    BackupUtils.deleteFileSet(backupItem, encodedRegionName, fileName, false);
                    return false;
                }
            }
        }
        catch (SQLException e) {
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.warn(LOG, "Failed to query Backup.Backup_FileSet table.");
            }
            return false;
        }

        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG,
                    "File %s can be removed from archived directory " +
                    "because it's not needed by incremental backup", fileName);
        }
        return false;
    }
}
