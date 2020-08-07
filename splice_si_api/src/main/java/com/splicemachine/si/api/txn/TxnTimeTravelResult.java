package com.splicemachine.si.api.txn;

public class TxnTimeTravelResult {
    private long txnId;
    private long ts;
    private boolean isBefore;
    private boolean isAfter;

    private TxnTimeTravelResult(long txnId, long ts, boolean isBefore, boolean isAfter) {
        this.txnId = txnId;
        this.ts = ts;
        this.isBefore = isBefore;
        this.isAfter = isAfter;
    }

    public boolean isBefore() {
        return isBefore;
    }

    public boolean isAfter() {
        return isAfter;
    }

    public long getTxnId() {
        return txnId;
    }

    public long getTs() {
        return ts;
    }

    public static TxnTimeTravelResult before() {
        return new TxnTimeTravelResult(Long.MAX_VALUE, Long.MAX_VALUE, true, false);
    }

    public static TxnTimeTravelResult after() {
        return new TxnTimeTravelResult(Long.MAX_VALUE, Long.MAX_VALUE, false, true);
    }

    public static TxnTimeTravelResult result(long txnId, long ts) {
        return new TxnTimeTravelResult(txnId, ts, false, false);
    }
}
