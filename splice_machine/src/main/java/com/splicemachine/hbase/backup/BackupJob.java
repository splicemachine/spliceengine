package com.splicemachine.hbase.backup;

import java.sql.Timestamp;

/**
 * Created by jyuan on 4/13/15.
 */
public class BackupJob {
    private long jobId;
    private String fileSystem;
    private String type;
    private int hourOfDay;
    private Timestamp beginTimestamp;

    public BackupJob(long jobId) {
        this.jobId = jobId;
    }

    public BackupJob(long jobId,
                     String fileSystem,
                     String type,
                     int hourOfDay,
                     Timestamp beginTimestamp) {
        this.jobId = jobId;
        this.fileSystem = fileSystem;
        this.type = type;
        this.hourOfDay = hourOfDay;
        this.beginTimestamp = beginTimestamp;
    }

    public long getJobId() {
        return jobId;
    }

    public String getFileSystem() {
        return fileSystem;
    }

    public String getType() {
        return type;
    }

    public int getHourOfDay() {
        return hourOfDay;
    }

    public Timestamp getBeginTimestamp() {
        return beginTimestamp;
    }
}
