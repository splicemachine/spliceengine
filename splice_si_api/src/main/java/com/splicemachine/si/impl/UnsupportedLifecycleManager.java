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
 *         Date: 7/7/14
 */
public class UnsupportedLifecycleManager implements TxnLifecycleManager {
	public static final UnsupportedLifecycleManager INSTANCE = new UnsupportedLifecycleManager();

	private UnsupportedLifecycleManager(){} //don't waste memory on me

	@Override
	public Txn beginTransaction() throws IOException {
		return beginTransaction((byte[])null);
	}

	@Override
	public Txn beginTransaction(byte[] destinationTable) throws IOException {
		return beginTransaction(Txn.IsolationLevel.SNAPSHOT_ISOLATION,destinationTable);
	}

	@Override
	public Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException {
		return beginTransaction(isolationLevel,null);
	}

	@Override
	public Txn beginTransaction(Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
		return beginChildTransaction(Txn.ROOT_TRANSACTION,isolationLevel,destinationTable);
	}

	@Override
	public Txn beginChildTransaction(TxnView parentTxn, byte[] destinationTable) throws IOException {
		return beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,null);
	}

	@Override
	public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
		return beginChildTransaction(parentTxn,isolationLevel, false,destinationTable);
	}

	@Override
	public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable) throws IOException {
		throw new UnsupportedOperationException("Cannot create new transactions from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, boolean inMemory) throws IOException {
		throw new UnsupportedOperationException("Cannot create new transactions from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, boolean inMemory, TaskId taskId) throws IOException {
		throw new UnsupportedOperationException("Cannot create new transactions from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public Txn elevateTransaction(Txn txn, byte[] destinationTable) throws IOException {
		throw new UnsupportedOperationException("Cannot elevate a transaction from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public long commit(long txnId) throws IOException {
		throw new UnsupportedOperationException("Cannot commit a transaction from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public void rollback(long txnId) throws IOException {
		throw new UnsupportedOperationException("Cannot rollback a transaction from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public void unregisterActiveTransaction(long txnId) throws IOException {
		throw new UnsupportedOperationException("Cannot unregister active transactions from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public void rollbackSubtransactions(long txnId, LongHashSet rolledback) throws IOException {
		throw new UnsupportedOperationException("Cannot rollback subtransactions from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public Txn chainTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean additive, byte[] destinationTable, Txn txnToCommit) throws IOException {
		throw new UnsupportedOperationException("Cannot chain a transaction from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public void enterRestoreMode() {
		throw new UnsupportedOperationException("Cannot enter restore mode from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public boolean isRestoreMode() {
		throw new UnsupportedOperationException("Cannot return restore mode from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}
	@Override
	public void setReplicationRole (String role) {
		throw new UnsupportedOperationException("Cannot set replication role from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}

	@Override
	public String getReplicationRole() {
		throw new UnsupportedOperationException("Cannot get replication role from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
	}
}
