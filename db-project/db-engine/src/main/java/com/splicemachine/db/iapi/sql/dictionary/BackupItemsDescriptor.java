package com.splicemachine.db.iapi.sql.dictionary;

import org.joda.time.DateTime;

/**
 * Created by jyuan on 2/6/15.
 */
public class BackupItemsDescriptor extends TupleDescriptor {
    private long backupId;
    private String item;
    private DateTime beginTimestamp;
    private DateTime endTimestamp;

    public BackupItemsDescriptor(long backupId,
                                 String item,
                                 DateTime beginTimestamp,
                                 DateTime endTimestamp) {
        this.backupId = backupId;
        this.item = item;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public long getBackupId() {
        return backupId;
    }

    public String getItem() {
        return item;
    }

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }

    public DateTime getEndTimestamp() {
        return endTimestamp;
    }
}

