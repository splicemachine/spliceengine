package com.splicemachine.backup;

/**
 * Created by jyuan on 8/8/16.
 */
import org.apache.hadoop.hbase.util.Bytes;

public class BackupRestoreConstants {

    public static String BACKUP_DIR = "backup";
    public static final String BACKUP_RECORD_FILE_NAME = "SYSBACKUP";
    public static final String RESTORE_RECORD_FILE_NAME = "SYSRESTORE";
    public static final String REGION_FILE_NAME = ".regioninfo";
    public static final String ARCHIVE_DIR = "archive";
    public static final byte[] BACKUP_TYPE_FULL_BYTES = Bytes.toBytes("FULL");
    public static final byte[] BACKUP_TYPE_INCR_BYTES = Bytes.toBytes("INCR");
    public static final String BACKUP_JOB_GROUP = "Backup";
    public static final String RESTORE_JOB_GROUP = "Restore";
    public static final long BACKUP_JOB_TIMEOUT = 30000;
}
