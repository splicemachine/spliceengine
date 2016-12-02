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

import com.carrotsearch.hppc.LongOpenHashSet;
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
	public void rollbackSubtransactions(long txnId, LongOpenHashSet rolledback) throws IOException {
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
}
