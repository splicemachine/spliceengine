package com.splicemachine.hbase.backup;

import com.splicemachine.derby.hbase.SpliceDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by jyuan on 1/29/15.
 */
public class BackupHFileCleaner extends BaseHFileCleanerDelegate {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }

    @Override
    public synchronized boolean isFileDeletable(FileStatus fStat) {
        String path = fStat.getPath().toString();
        String[] s = path.split("/");
        int n = s.length;
        Long conglomId = new Long(s[n-4]);
        String encodedRegionName = s[n-3];
        String fileName = s[n-1];

        return (!shouldRetainForBackup(conglomId, encodedRegionName, fileName));
    }


    private boolean shouldRetainForBackup(long conglomId, String encodedRegionName, String fileName){
        Connection connection = null;

        try{
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(
                    String.format(BackupUtils.QUERY_FILE_SET, BackupUtils.FILESET_SCHEMA, BackupUtils.FILESET_TABLE));
            ps.setLong(1, conglomId);
            ps.setString(2, encodedRegionName);
            ps.setString(3, fileName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        }
        catch (SQLException e) {
            return false;
        }

        return false;
    }
}
