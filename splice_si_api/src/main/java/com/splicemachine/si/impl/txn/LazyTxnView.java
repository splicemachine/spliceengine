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

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * @author Scott Fines
 * Date: 6/19/14
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
public class LazyTxnView implements TxnView {
    private volatile TxnView delegate;
    private volatile boolean lookedUp = false;
    private volatile  TxnSupplier store;
    private final ExceptionFactory exceptionFactory;
    private final long txnId;
    private final boolean hasAdditive;
    private final boolean additive;
    private final Txn.IsolationLevel isolationLevel;
    private volatile boolean inFinalState = false; //set to true if/when the lookup reveals the transaction is in the final state

    public LazyTxnView(long txnId, TxnSupplier store,ExceptionFactory exceptionFactory) {
        this.txnId = txnId;
        this.store = store;
        this.exceptionFactory = exceptionFactory;

        this.hasAdditive = false;
        this.additive = false;
        this.isolationLevel = null;
    }

    public LazyTxnView(long txnId, TxnSupplier store,
                       boolean hasAdditive, boolean additive,
                       Txn.IsolationLevel isolationLevel,
                       ExceptionFactory exceptionFactory) {
        this.store = store;
        this.txnId = txnId;
        this.hasAdditive = hasAdditive;
        this.additive = additive;
        this.isolationLevel = isolationLevel;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public long getGlobalCommitTimestamp() {
        lookup(!inFinalState);
        return delegate.getGlobalCommitTimestamp();
    }

    @Override
    public ConflictType conflicts(TxnView otherTxn) {
        lookup(!inFinalState);
        return delegate.conflicts(otherTxn);
    }

    @Override
    public boolean isAdditive() {
        if(hasAdditive) return additive;
        lookup(false); //don't need to force a refresh, since this property is constant for this transaction
        return delegate.isAdditive();
    }

    @Override
    public Iterator<ByteSlice> getDestinationTables() {
        lookup(!inFinalState);
        return delegate.getDestinationTables();
    }

    @Override
    public boolean descendsFrom(TxnView potentialParent) {
        lookup(false);
        return delegate.descendsFrom(potentialParent);
    }

    @Override
    public Txn.State getEffectiveState() {
        lookup(!inFinalState);
        return delegate.getEffectiveState();
    }

    @Override
    public Txn.IsolationLevel getIsolationLevel() {
        if(isolationLevel!=null) return isolationLevel;
        lookup(false); //isolation levl never changes
        return delegate.getIsolationLevel();
    }

    @Override
    public long getTxnId() {
        return txnId;
    }

    @Override
    public boolean allowsSubtransactions() {
        return false;
    }

    @Override
    public int getSubId() {
        return (int)(txnId & 0xFF);
    }

    @Override
    public long getBeginTimestamp() {
        /*
         * As of this comment (Sept. 2014) the begin timestamp and the
         * transaction id are the same thing. Therefore, we can defer
         * one from the other. However, should that change(e.g. because
         * we create some other form of Lamport clock to determine transactional
         * relationships), we will need to re-implement this method.
         */
        return txnId;
//        lookup(false); //begin timestamp never changes
//        return delegate.getBeginTimestamp();
    }

    @Override
    public long getCommitTimestamp() {
        lookup(!inFinalState);
        return delegate.getCommitTimestamp();
    }

    @Override
    public long getEffectiveCommitTimestamp() {
        lookup(!inFinalState);
        return delegate.getEffectiveCommitTimestamp();
    }

    @Override
    public long getEffectiveBeginTimestamp() {
        lookup(false);
        return delegate.getEffectiveBeginTimestamp();
    }

    @Override
    public long getLastKeepAliveTimestamp() {
        return -1l; //don't lookup just for the timestamp;
    }

    @Override
    public TxnView getParentTxnView() {
        lookup(false);
        return delegate.getParentTxnView();
    }

    @Override
    public long getParentTxnId() {
        return getParentTxnView().getParentTxnId();
    }

    @Override
    public Txn.State getState() {
        lookup(!inFinalState);
        return delegate.getState();
    }

    @Override
    public boolean allowsWrites() {
        lookup(false); //never changes
        return delegate.allowsWrites();
    }

    @Override
    public boolean canSee(TxnView otherTxn) {
        lookup(!inFinalState);
        return delegate.canSee(otherTxn);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TxnView)) return false;

        TxnView other = (TxnView) o;

        return txnId == other.getTxnId();

    }

    @Override
    public int hashCode() {
        return (int) (txnId ^ (txnId >>> 32));
    }

    /**
     * @return the eager transaction view for this transaction
     */
    public TxnView getDelegate(){
        lookup(!inFinalState); //ensure that we are present
        return delegate;
    }


    public void setSupplier(TxnSupplier supplier) { this.store = supplier; }

    @Override
    public String toString() {
        lookup(!inFinalState);
        return "Lazy" + delegate;
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
	}


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void lookup(boolean force)  {
        if(lookedUp&&!force) return; //no need to perform lookup
        synchronized (this){
            if(lookedUp&&!force) return; //double checked locking to avoid race conditions on committed transactions
            try {
                delegate = store.getTransaction(txnId);
                if(delegate==null)
                    throw exceptionFactory.readOnlyModification("Txn "+txnId+" is read only");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            lookedUp = true;
            inFinalState = delegate.getCommitTimestamp()>=0;
        }
    }
}
