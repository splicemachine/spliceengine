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
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/24/14
 */
public class ForwardingLifecycleManager implements TxnLifecycleManager{
    private final TxnLifecycleManager lifecycleManager;

    public ForwardingLifecycleManager(TxnLifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public Txn beginTransaction() throws IOException {
        Txn txn = lifecycleManager.beginTransaction();
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginTransaction(byte[] destinationTable) throws IOException {
        Txn txn = lifecycleManager.beginTransaction(destinationTable);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException {
        Txn txn = lifecycleManager.beginTransaction(isolationLevel);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginTransaction(Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
        Txn txn = lifecycleManager.beginTransaction(isolationLevel, destinationTable);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn, byte[] destinationTable) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn, destinationTable);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, destinationTable);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, boolean inMemory) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable, inMemory);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, boolean inMemory, TaskId taskId) throws IOException {
        Txn txn = lifecycleManager.beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable, inMemory, taskId);
        afterStart(txn);
        return txn;
    }

    @Override
    public Txn chainTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, Txn txnToCommit) throws IOException {
        Txn txn = lifecycleManager.chainTransaction(parentTxn,isolationLevel, additive,destinationTable,txnToCommit);
        afterStart(txn);
        return txn;
    }

    @Override
    public void enterRestoreMode() {
        lifecycleManager.enterRestoreMode();
    }

    @Override
    public boolean isRestoreMode() {
        return lifecycleManager.isRestoreMode();
    }

    @Override
    public void setReplicationRole (String role) {
        lifecycleManager.setReplicationRole(role);
    }

    @Override
    public String getReplicationRole() {
        return lifecycleManager.getReplicationRole();
    }
    @Override
    public Txn elevateTransaction(Txn txn, byte[] destinationTable) throws IOException {
        Txn txn1 = lifecycleManager.elevateTransaction(txn, destinationTable);
        afterStart(txn1);
        return txn1;
    }

    @Override
    public long commit(long txnId) throws IOException {
        return lifecycleManager.commit(txnId);
    }

    @Override
    public void rollback(long txnId) throws IOException {
        lifecycleManager.rollback(txnId);
    }

    @Override
    public void unregisterActiveTransaction(long txnId) throws IOException {
        lifecycleManager.unregisterActiveTransaction(txnId);
    }

    @Override
    public void rollbackSubtransactions(long txnId, LongHashSet rolledback) throws IOException {
        lifecycleManager.rollbackSubtransactions(txnId, rolledback);
    }


    protected void afterStart(Txn txn){
        //no-op by default
    }

}
