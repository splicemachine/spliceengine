package com.splicemachine.ck.encoder;

public class RPutConfig {

    private boolean tombstone = false;
    private boolean antiTombstone = false;
    private boolean firstWrite = false;
    private boolean deleteAfterFirstWrite = false;
    private Long foreignKeyCounter = null;
    private String userData = null;
    private Long txnId = null;
    private Long commitTS = null;

    public boolean hasTombstone() {
        return tombstone;
    }

    public void setTombstone() {
        this.tombstone = true;
    }

    public boolean hasAntiTombstone() {
        return antiTombstone;
    }

    public void setAntiTombstone() {
        this.antiTombstone = true;
    }

    public boolean hasFirstWrite() {
        return firstWrite;
    }

    public void setFirstWrite() {
        this.firstWrite = true;
    }

    public boolean hasDeleteAfterFirstWrite() {
        return deleteAfterFirstWrite;
    }

    public void setDeleteAfterFirstWrite() {
        this.deleteAfterFirstWrite = true;
    }

    public boolean hasForeignKeyCounter() {
        return foreignKeyCounter != null;
    }

    public long getForeignKeyCounter() {
        assert foreignKeyCounter != null;
        return foreignKeyCounter;
    }

    public void setForeignKeyCounter(long foreignKeyCounter) {
        this.foreignKeyCounter = foreignKeyCounter;
    }

    public boolean hasUserData() {
        return userData != null;
    }

    public String getUserData() {
        return userData;
    }

    public void setUserData(String userData) {
        this.userData = userData;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setCommitTS(long commitTS) {
        this.commitTS = commitTS;
    }

    public boolean hasCommitTS() {
        return commitTS == null;
    }

    public long getCommitTS() {
        return commitTS;
    }
}
