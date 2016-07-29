package com.splicemachine.backup;

import com.splicemachine.db.iapi.sql.dictionary.BackupDescriptor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by jyuan on 8/10/16.
 */
public class BackupUtils {

    /**
     *
     * @param fs HBase file system
     * @param rootPath HBase root directory
     * @return True if there exists a successful database backup
     * @throws IOException
     */
    public static boolean existsDatabaseBackup(FileSystem fs, Path rootPath) throws IOException {
        boolean ret = false;
        FSDataInputStream in = null;
        try {
            // Open backup record file from file system
            Path backupPath = new Path(rootPath, BackupRestoreConstants.BACKUP_DIR);
            Path p = new Path(backupPath, BackupRestoreConstants.BACKUP_RECORD_FILE_NAME);
            if (fs.exists(p)) {
                in = fs.open(p);
                int n = in.readInt();  // number of records
                BackupDescriptor bd = new BackupDescriptor();
                while (n-- > 0) {
                    bd.readExternal(in);
                    if (bd.getScope().compareToIgnoreCase("DATABASE") == 0) {
                        ret = true;
                        break;
                    }
                }
            }
            return ret;
        }
        finally {
            if (in != null)
                in.close();
        }
    }

    /**
     * Given a file path for an archived HFile, return the corresponding file path in backup metadata directory
     * @param p file path for archived HFile
     * @return orresponding file path in backup metadata directory
     */
    public static String getBackupFilePath(String p) {
        return p.replace(BackupRestoreConstants.ARCHIVE_DIR, BackupRestoreConstants.BACKUP_DIR);
    }
}
