package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import org.joda.time.DateTime;

/**
 * Created by jyuan on 2/6/15.
 */
public class BackupItemsDescriptor extends TupleDescriptor {
    private long txnId;
    private String item;
    private DateTime beginTimestamp;
    private DateTime endTimestamp;
    private String snapshotName;

    public BackupItemsDescriptor(long txnId,
                                 String item,
                                 DateTime beginTimestamp,
                                 DateTime endTimestamp,
                                 String snapshotName) {
        this.txnId = txnId;
        this.item = item;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
        this.snapshotName = snapshotName;
    }

    public long getTxnId() {
        return txnId;
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

    public String getSnapshotName() {
        return snapshotName;
    }
}

