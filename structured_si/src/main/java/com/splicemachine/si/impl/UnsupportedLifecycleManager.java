package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;

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
				return beginChildTransaction(parentTxn,isolationLevel,false,destinationTable);
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean dependent, byte[] destinationTable) throws IOException {
				return beginChildTransaction(parentTxn,isolationLevel,false,false,destinationTable);
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean isDependent, boolean additive, byte[] destinationTable) throws IOException {
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
		public Txn chainTransaction(Txn parentTxn, Txn.IsolationLevel isolationLevel, boolean dependent, boolean additive, byte[] destinationTable, Txn txnToCommit) throws IOException {
				throw new UnsupportedOperationException("Cannot chain a transaction from the UnsupportedLifecycle Manager. Use a real Lifecycle manager instead");
		}
}
