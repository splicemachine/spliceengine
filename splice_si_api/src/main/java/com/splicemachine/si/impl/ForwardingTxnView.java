/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;

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

    @Override public boolean isAdditive() { return delegate.isAdditive(); }
    @Override public Iterator<ByteSlice> getDestinationTables() { return delegate.getDestinationTables(); }
}
