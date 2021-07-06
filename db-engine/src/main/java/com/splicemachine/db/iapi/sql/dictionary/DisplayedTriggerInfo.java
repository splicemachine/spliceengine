package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.UUID;

public class DisplayedTriggerInfo {
    private UUID id;
    private String name;
    private long txnId;
    private java.util.UUID queryId;
    private java.util.UUID parentQueryId;
    private long elapsedTime = -1;
    private long modifiedRowCount = -1;

    public DisplayedTriggerInfo(UUID id, String name, long txnId, java.util.UUID queryId) {
        this.id = id;
        this.name = name;
        this.txnId = txnId;
        this.queryId = queryId;
    }

    public DisplayedTriggerInfo(UUID id, String name, long txnId, java.util.UUID queryId, java.util.UUID parentQueryId) {
        this(id, name, txnId, queryId);
        this.parentQueryId = parentQueryId;
    }


    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getTxnId() {
        return txnId;
    }

    public java.util.UUID getQueryId() {
        return queryId;
    }

    public void setQueryId(java.util.UUID queryId) {
        this.queryId = queryId;
    }

    public java.util.UUID getParentQueryId() {
        return parentQueryId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public long getModifiedRowCount() {
        return modifiedRowCount;
    }

    public void setModifiedRowCount(long modifiedRowCount) {
        this.modifiedRowCount = modifiedRowCount;
    }
}
