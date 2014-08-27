package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.util.Iterator;

/**
 * A view of a transaction that has slightly stronger visibility semantics.
 *
 * In particular, this uses normal visibility semantics *and* an additional "demarcation point".
 * Only transactions which are visible to the underlying transaction *and* began before
 * the begin timestamp of the transaction are visible through this view.
 *
 * This is primarily useful for the second stage of DDL operations, such as populating an index or
 * manipulating back data, in that it ensures that a clear demarcation point in time is made.
 *
 * @author Scott Fines
 * Date: 8/27/14
 */
public class DemarcatedTxnView implements TxnView {
    private final TxnView txn;
    private final long demarcationPoint;

    public DemarcatedTxnView(TxnView delegate,long demarcationPoint) {
        this.txn = delegate;
        this.demarcationPoint = demarcationPoint;
        assert demarcationPoint>=0: "Cannot use a demarcated view with a negative demarcation point";
    }

    @Override public Txn.State getEffectiveState() { return txn.getEffectiveState(); }
    @Override public Txn.IsolationLevel getIsolationLevel() { return txn.getIsolationLevel(); }
    @Override public long getTxnId() { return txn.getTxnId(); }
    @Override public long getBeginTimestamp() { return txn.getBeginTimestamp(); }
    @Override public long getCommitTimestamp() { return txn.getCommitTimestamp(); }
    @Override public long getEffectiveCommitTimestamp() { return txn.getEffectiveCommitTimestamp(); }
    @Override public long getEffectiveBeginTimestamp() { return txn.getEffectiveBeginTimestamp(); }
    @Override public long getLastKeepAliveTimestamp() { return txn.getLastKeepAliveTimestamp(); }
    @Override public TxnView getParentTxnView() { return txn.getParentTxnView(); }
    @Override public long getParentTxnId() { return txn.getParentTxnId(); }
    @Override public Txn.State getState() { return txn.getState(); }
    @Override public boolean allowsWrites() { return txn.allowsWrites(); }
    @Override public boolean isAdditive() { return txn.isAdditive(); }
    @Override public long getGlobalCommitTimestamp() { return txn.getGlobalCommitTimestamp(); }
    @Override public Iterator<ByteSlice> getDestinationTables() { return txn.getDestinationTables(); }
    @Override public boolean descendsFrom(TxnView potentialParent) { return txn.descendsFrom(potentialParent); }

    @Override
    public boolean canSee(TxnView otherTxn) {
        return txn.canSee(otherTxn) && otherTxn.getBeginTimestamp() < demarcationPoint;
    }

    @Override
    public ConflictType conflicts(TxnView otherTxn) {
        return txn.conflicts(otherTxn);
    }

}
