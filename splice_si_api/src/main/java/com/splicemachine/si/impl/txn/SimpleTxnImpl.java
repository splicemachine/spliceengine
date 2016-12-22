package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.ChildStatementDuration;
import com.splicemachine.si.api.txn.TransactionStatus;
import com.splicemachine.si.api.txn.Txn;

/**
 * Created by jleach on 12/20/16.
 */
public class SimpleTxnImpl implements Txn {

    @Override
    public long getTxnId() {
        return 0;
    }

    @Override
    public long getParentTxnId() {
        return 0;
    }

    @Override
    public long getCommitTimestamp() {
        return 0;
    }

    @Override
    public int getNodeId() {
        return 0;
    }

    @Override
    public int getRegionId() {
        return 0;
    }

    @Override
    public long getDuration() {
        return 0;
    }

    @Override
    public long[] getRolledBackChildIds() {
        return new long[0];
    }

    @Override
    public ChildStatementDuration getChildStatementDuration() {
        return null;
    }

    @Override
    public long getHLCTimestamp() {
        return 0;
    }

    @Override
    public String getUserId() {
        return null;
    }

    @Override
    public String getStatementId() {
        return null;
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return null;
    }

    @Override
    public boolean isPersisted() {
        return false;
    }
}
