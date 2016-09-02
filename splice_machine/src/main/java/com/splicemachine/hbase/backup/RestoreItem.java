package com.splicemachine.hbase.backup;

/**
 * Created by jyuan on 4/16/15.
 */
public class RestoreItem {
    private String item;
    private long beginTransactionId;
    private long commitTransactionId;

    public RestoreItem() {

    }

    public RestoreItem(String item,
                       long beginTransactionId,
                       long commitTransactionId) {
        this.item = item;
        this.beginTransactionId = beginTransactionId;
        this.commitTransactionId = commitTransactionId;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public long getBeginTransactionId() {
        return beginTransactionId;
    }

    public void setBeginTransactionId(long beginTransactionId) {
        this.beginTransactionId = beginTransactionId;
    }

    public long getCommitTransactionId() {
        return commitTransactionId;
    }

    public void setCommitTransactionId(long commitTransactionId) {
        this.commitTransactionId = commitTransactionId;
    }
}
