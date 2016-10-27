package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

/**
 * Created by jyuan on 1/29/15.
 */
public class SpliceHFileCleaner extends BaseHFileCleanerDelegate {

    private static final Logger LOG = Logger.getLogger(SpliceHFileCleaner.class);

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }

    @Override
    public synchronized boolean isFileDeletable(FileStatus fStat) {

        boolean deletable = true;
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            Path rootDir = FSUtils.getRootDir(conf);
            FileSystem fs = FSUtils.getCurrentFileSystem(conf);
            /**An archived HFile is reserved for an incremental backup if
             * 1) There exists a successful full/incremental backup for the database
             * 2) An empty file with the same name exists in backup directory.
            */
            if (BackupUtils.existsDatabaseBackup(fs, rootDir)) {
                String p = BackupUtils.getBackupFilePath(fStat.getPath().toString());
                if (fs.exists(new Path(p)))
                    deletable = false;
            }
        }
        catch(Exception e) {
            //ignores any exception
        }
        return deletable;
    }
}
