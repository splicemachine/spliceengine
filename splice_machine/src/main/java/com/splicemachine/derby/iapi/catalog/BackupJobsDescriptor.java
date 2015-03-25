package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import org.joda.time.DateTime;

/**
 * Created by jyuan on 3/24/15.
 */
public class BackupJobsDescriptor extends TupleDescriptor{

    private long jobId;
    private String fileSystem;
    private String type;
    private int hourOfDay;
    private DateTime beginTimestamp;

    public BackupJobsDescriptor () {}

    public BackupJobsDescriptor (long jobId,
                                 String fileSystem,
                                 String type,
                                 int hourOfDay,
                                 DateTime beginTimestamp) {
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

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }
}
