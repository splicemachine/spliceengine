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

package com.splicemachine.si.impl.txn;

import com.carrotsearch.hppc.LongHashSet;
import com.splicemachine.db.iapi.sql.dictionary.DisplayedTriggerInfo;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn extends AbstractTxnView implements Txn {

    private AtomicLong counter;
    protected LongHashSet rolledback = new LongHashSet();
    protected Set<Txn> children = ConcurrentHashMap.newKeySet();
    protected Txn parentReference;
    private boolean subtransactionsAllowed = true;
    private AtomicInteger numTriggers;
    private ArrayList<DisplayedTriggerInfo> triggerInfos = new ArrayList<>();
    private java.util.UUID currentQueryId;
    private HashMap<com.splicemachine.db.catalog.UUID, DisplayedTriggerInfo> triggerIdToTriggerInfoMap = new HashMap<>();

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
        this.numTriggers = new AtomicInteger(0);
    }

    @Override
    public void setNumTriggers(int num) {
        numTriggers.set(num);
    }

    @Override
    public int getNumTriggers() {
        return numTriggers.get();
    }

    @Override
    public void setCurrentQueryId(UUID id) {
        currentQueryId = id;
    }
    public java.util.UUID getCurrentQueryId() {
        return currentQueryId;
    }

    @Override
    public void incNumTriggers(TriggerDescriptor[] tds) {
        AbstractTxn parent;
        if (parentReference == null || parentReference.getCurrentQueryId() == null) {
            parent = (AbstractTxn) getParentTxnView();
            if (parent.getCurrentQueryId() == null) {
                return; // maybe should throw an error here
            }
        } else {
            parent = (AbstractTxn) parentReference;
        }

        for (TriggerDescriptor td : tds) {
            parent.triggerIdToTriggerInfoMap.get(td.getUUID()).setTxnId(txnId);
            parent.triggerIdToTriggerInfoMap.get(td.getUUID()).setQueryId(currentQueryId);
        }
    }

    @Override
    public ArrayList<DisplayedTriggerInfo> getDisplayedTriggerInfo() {
        ArrayList<DisplayedTriggerInfo> result = new ArrayList<>();
        result.addAll(triggerIdToTriggerInfoMap.values());
        result.addAll(triggerInfos);
        return result;
    }

    @Override
    public void addNumTriggers(int num, ArrayList<DisplayedTriggerInfo> triggerInfos) {
        numTriggers.getAndAdd(num);
        this.triggerInfos.addAll(triggerInfos);
    }

    @Override
    public void initTxnTriggers(TriggerDescriptor[] tds) {
        for (TriggerDescriptor td : tds) {
            numTriggers.getAndIncrement();
            triggerIdToTriggerInfoMap.put(td.getUUID(), new DisplayedTriggerInfo(td.getUUID(), td.getName(), -1, null, txnId, currentQueryId));
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
    public LongHashSet getRolledback() {
        if (getSubId() == 0) {
            return rolledback.clone();
        } else {
            return parentReference.getRolledback();
        }
    }

    private boolean internalAllowsSubtransactions() {
        AbstractTxn other = this;
        if (counter != null && counter.get() >= SIConstants.SUBTRANSANCTION_ID_MASK) {
            return false;
        }
        while (true) {
            if (!other.subtransactionsAllowed)
                return false;
            boolean nonRolledbackChild = false;
            if (!children.isEmpty()) {
                for (Txn c : other.children) {
                    if (c.getState() == State.ACTIVE) {
                        if (nonRolledbackChild) {
                            return false;
                        }
                        nonRolledbackChild = true;
                    }
                }
            }
            if (other.parentReference != null) {
                other = ((AbstractTxn) other.parentReference);
                continue;
            }
            return true;
        }
    }

    @Override
    public boolean allowsSubtransactions() {
        if (counter != null && counter.get() >= SIConstants.SUBTRANSANCTION_ID_MASK) {
            return false;
        }
        if (!children.isEmpty()) {
            for (Txn c : children) {
                if (c.getState() == State.ACTIVE) {
                    return false;
                }
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

    @Override
    public boolean canSee(TxnView otherTxn) {
        // Protects against reading data written by the "self-insert transaction"
        if (!children.isEmpty()) {
            for (Txn c : children) {
                if (c.getTxnId() == otherTxn.getTxnId() && c.getState() == State.ACTIVE) {
                    return false;
                }
            }
        }
        return super.canSee(otherTxn);
    }

    @Override
    public TaskId getTaskId() {
        return null;
    }

    @Override
    public void commit() throws IOException{
        triggerInfos.addAll(triggerIdToTriggerInfoMap.values());
        if (parentReference != null) {
            parentReference.addNumTriggers(numTriggers.get(), triggerInfos);
        }
    }
}
