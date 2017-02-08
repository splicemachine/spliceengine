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

import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn {

    private AtomicLong counter;
    protected LongOpenHashSet rolledback = new LongOpenHashSet();
    protected Set<Txn> children = new HashSet<>();
    protected Txn parentReference;
    private boolean subtransactionsAllowed = true;

    protected AbstractTxn(){
    }

    protected AbstractTxn(
            Txn parentReference,
            long txnId,
            long beginTimestamp,
            IsolationLevel isolationLevel){
        super(txnId,beginTimestamp,isolationLevel);
        if (parentReference != null) {
            this.parentReference = parentReference;
            this.parentReference.register(this);
        }
        if (getSubId() == 0) {
            counter = new AtomicLong(0);
        }
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        throw new UnsupportedOperationException("Transactions cannot be serialized, only their views");
    }

    @Override
    public long newSubId() {
        if (getSubId() == 0) {
            return counter.incrementAndGet();
        } else {
            return parentReference.newSubId();
        }
    }

    public Txn getParentReference() {
        return parentReference;
    }

    @Override
    public void register(Txn child) {
        children.add(child);
    }

    @Override
    public void addRolledback(long subId) {
        if (getSubId() == 0) {
            rolledback.add(subId);
        } else {
            parentReference.addRolledback(subId);
        }
    }

    @Override
    public LongOpenHashSet getRolledback() {
        if (getSubId() == 0) {
            return rolledback.clone();
        } else {
            return parentReference.getRolledback();
        }
    }

    private boolean internalAllowsSubtransactions() {
        if (!subtransactionsAllowed)
            return false;
        boolean nonRolledbackChild = false;
        for (Txn c : children) {
            if (c.getState() == State.ACTIVE) {
                if (nonRolledbackChild) {
                    return false;
                }
                nonRolledbackChild = true;
            }
        }
        if (parentReference != null)
            return ((AbstractTxn)parentReference).internalAllowsSubtransactions();
        return true;
    }

    @Override
    public boolean allowsSubtransactions() {
        if (!subtransactionsAllowed)
            return false;
        for (Txn c : children) {
            if (c.getState() == State.ACTIVE) {
                return false;
            }
        }
        if (parentReference != null)
            return ((AbstractTxn)parentReference).internalAllowsSubtransactions();
        return true;
    }

    @Override
    public void forbidSubtransactions() {
        subtransactionsAllowed = false;
        if (parentReference != null) {
            parentReference.forbidSubtransactions();
        }
    }
}
