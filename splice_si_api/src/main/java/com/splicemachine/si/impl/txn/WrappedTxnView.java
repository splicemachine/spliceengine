package com.splicemachine.si.impl.txn;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * Created by jleach on 10/20/17.
 */
public class WrappedTxnView implements TxnView {
    private TxnView txnView;
    public WrappedTxnView(TxnView txnView) {
        this.txnView = txnView;
    }

    @Override
    public Txn.State getEffectiveState() {
        return txnView.getEffectiveState();
    }

    @Override
    public Txn.IsolationLevel getIsolationLevel() {
        return txnView.getIsolationLevel();
    }

    @Override
    public long getTxnId() {
        return txnView.getTxnId();
    }

    @Override
    public boolean allowsSubtransactions() {
        return txnView.allowsSubtransactions();
    }

    @Override
    public boolean equivalent(TxnView o) {
        return txnView.equivalent(o);
    }

    @Override
    public int getSubId() {
        return txnView.getSubId();
    }

    @Override
    public long getBeginTimestamp() {
        return txnView.getBeginTimestamp();
    }

    @Override
    public long getCommitTimestamp() {
        return txnView.getCommitTimestamp();
    }

    @Override
    public long getEffectiveCommitTimestamp() {
        return txnView.getEffectiveCommitTimestamp();
    }

    @Override
    public long getEffectiveBeginTimestamp() {
        return txnView.getEffectiveBeginTimestamp();
    }

    @Override
    public long getLastKeepAliveTimestamp() {
        return txnView.getLastKeepAliveTimestamp();
    }

    @Override
    public TxnView getParentTxnView() {
        return txnView.getParentTxnView();
    }

    @Override
    public long getParentTxnId() {
        return txnView.getParentTxnId();
    }

    @Override
    public Txn.State getState() {
        return txnView.getState();
    }

    @Override
    public boolean allowsWrites() {
        return txnView.allowsWrites();
    }

    @Override
    public boolean canSee(TxnView otherTxn) {
        return txnView.canSee(otherTxn);
    }

    @Override
    public boolean isAdditive() {
        return txnView.isAdditive();
    }

    @Override
    public long getGlobalCommitTimestamp() {
        return txnView.getGlobalCommitTimestamp();
    }

    @Override
    public ConflictType conflicts(TxnView otherTxn) {
        return txnView.conflicts(otherTxn);
    }

    @Override
    public Iterator<ByteSlice> getDestinationTables() {
        return txnView.getDestinationTables();
    }

    @Override
    public boolean descendsFrom(TxnView potentialParent) {
        return txnView.descendsFrom(potentialParent);
    }

    @Override
    public TxnView getReadUncommittedActiveTxn() {
        return txnView.getReadUncommittedActiveTxn();
    }

    @Override
    public TxnView getReadCommittedActiveTxn() {
        return txnView.getReadCommittedActiveTxn();
    }

    @Override
    public LongOpenHashSet getRolledback() {
        return txnView.getRolledback();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        txnView.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txnView.readExternal(in);
    }

    @Override
    public String toString() {
        return txnView.toString();
    }
}
