package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 8/20/14
 */
public abstract class ForwardingTxnView extends AbstractTxn{
    protected final Txn delegate;

    protected ForwardingTxnView(Txn delegate) {
        super(delegate.getTxnId(),delegate.getBeginTimestamp(),delegate.getIsolationLevel());
        this.delegate = delegate;
    }

    @Override
    public String toString() {
        return "Txn("+getTxnId()+","+getState()+")";
    }


    @Override
    public long getGlobalCommitTimestamp() {
        return 0;
    }

    @Override public void commit() throws IOException { delegate.commit(); }
    @Override public void rollback() throws IOException { delegate.rollback(); }
    @Override public Txn elevateToWritable(byte[] writeTable) throws IOException { return delegate.elevateToWritable(writeTable); }
    @Override public Txn.IsolationLevel getIsolationLevel() { return delegate.getIsolationLevel(); }
    @Override public long getTxnId() { return delegate.getTxnId(); }
    @Override public long getBeginTimestamp() { return delegate.getBeginTimestamp(); }
    @Override public long getCommitTimestamp() { return delegate.getCommitTimestamp(); }
    @Override public long getEffectiveBeginTimestamp() { return delegate.getEffectiveBeginTimestamp(); }
    @Override public long getLastKeepAliveTimestamp() { return delegate.getLastKeepAliveTimestamp(); }
    @Override public TxnView getParentTxnView() { return delegate.getParentTxnView(); }
    @Override public long getParentTxnId() { return delegate.getParentTxnId(); }
    @Override public Txn.State getState() { return delegate.getState(); }
    @Override public boolean allowsWrites() { return delegate.allowsWrites(); }

    @Override public boolean isAdditive() { return delegate.isAdditive(); }
    @Override public Iterator<ByteSlice> getDestinationTables() { return delegate.getDestinationTables(); }
}
