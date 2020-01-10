/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongHashSet;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.AbstractTxn;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 8/20/14
 */
public abstract class ForwardingTxnView extends AbstractTxn {
    private final Txn delegate;

    protected ForwardingTxnView(Txn delegate) {
        super(delegate.getParentReference(), delegate.getTxnId(),delegate.getBeginTimestamp(),delegate.getIsolationLevel());
        this.delegate = delegate;
    }

    @Override
    public String toString() {
        return "Forwarding" + delegate;
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
    @Override public void subRollback() { delegate.subRollback(); }
    @Override
    public LongHashSet getRolledback() {
        return delegate.getRolledback();
    }

    @Override public boolean isAdditive() { return delegate.isAdditive(); }
    @Override public Iterator<ByteSlice> getDestinationTables() { return delegate.getDestinationTables(); }

    @Override
    public boolean equivalent(TxnView o) {
        return delegate.equivalent(o);
    }

    @Override
    public TaskId getTaskId() {
        return delegate.getTaskId();
    }
}
