package com.splicemachine.derby.iapi.catalog;

/**
 * Created by jyuan on 2/6/15.
 */
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import org.joda.time.DateTime;

public class BackupDescriptor extends TupleDescriptor {

    private long txnId;
    private DateTime beginTimestamp;
    private DateTime endTimestamp;
    private String status;
    private String fileSystem;
    private String scope;
    private boolean isIncremental;
    private long parentId;
    private int items;

    public BackupDescriptor(long txnId,
                            DateTime beginTimestamp,
                            DateTime endTimestamp,
                            String status,
                            String fileSystem,
                            String scope,
                            boolean isIncremental,
                            long parentId,
                            int items) {
        this.txnId = txnId;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
        this.status = status;
        this.fileSystem = fileSystem;
        this.scope = scope;
        this.isIncremental = isIncremental;
        this.parentId = parentId;
        this.items = items;
    }

    public long getTxnId() {
        return txnId;
    }

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }

    public DateTime getEndTimestamp() {
        return endTimestamp;
    }

    public String getStatus() {
        return status;
    }

    public String getFileSystem() {
        return fileSystem;
    }

    public String getScope() {
        return scope;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    public long getParentBackupId() {
        return parentId;
    }

    public int getItems() {
        return items;
    }
}
