package com.splicemachine.si;

import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.TransactionId;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
public abstract class ForwardingTransactionManager  implements TransactionManager{
		private final TransactionManager delegate;

		protected ForwardingTransactionManager(TransactionManager delegate) {
				this.delegate = delegate;
		}

		@Override public TransactionId beginTransaction() throws IOException { return delegate.beginTransaction(); }
		@Override public TransactionId beginTransaction(boolean allowWrites) throws IOException { return delegate.beginTransaction(allowWrites); }
		@Override public TransactionId beginTransaction(byte[] writeTable) throws IOException { return delegate.beginTransaction(writeTable); }

		@Override
		public TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException {
				return delegate.beginTransaction(allowWrites, readUncommitted, readCommitted);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException {
				return delegate.beginChildTransaction(parent, allowWrites);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, byte[] writeTable) throws IOException {
				return delegate.beginChildTransaction(parent,writeTable);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException {
				return delegate.beginChildTransaction(parent, dependent, allowWrites);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, byte[] table) throws IOException {
				return delegate.beginChildTransaction(parent,dependent,table);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites, boolean additive, Boolean readUncommitted, Boolean readCommitted, TransactionId transactionToCommit) throws IOException {
				return delegate.beginChildTransaction(parent, dependent, allowWrites, additive, readUncommitted, readCommitted, transactionToCommit);
		}

		@Override
		public TransactionId beginChildTransaction(TransactionId parent,
																							 boolean dependent,
																							 boolean additive,
																							 Boolean readUncommitted,
																							 Boolean readCommitted,
																							 TransactionId transactionToCommit,
																							 byte[] writeTable) throws IOException {
				return delegate.beginChildTransaction(parent,dependent,additive,readUncommitted,readCommitted,transactionToCommit,writeTable);
		}

		@Override public void keepAlive(TransactionId transactionId) throws IOException { delegate.keepAlive(transactionId); }
		@Override public void commit(TransactionId transactionId) throws IOException { delegate.commit(transactionId); }
		@Override public void rollback(TransactionId transactionId) throws IOException { delegate.rollback(transactionId); }
		@Override public void fail(TransactionId transactionId) throws IOException { delegate.fail(transactionId); }
		@Override public TransactionStatus getTransactionStatus(TransactionId transactionId) throws IOException { return delegate.getTransactionStatus(transactionId); }
		@Override public TransactionId transactionIdFromString(String transactionId) { return delegate.transactionIdFromString(transactionId); }
		@Override public List<TransactionId> getActiveTransactionIds(TransactionId max) throws IOException { return delegate.getActiveTransactionIds(max); }

		@Override
		public List<TransactionId> getActiveWriteTransactionIds(TransactionId max, byte[] table) throws IOException {
				return delegate.getActiveWriteTransactionIds(max,table);
		}

		@Override public boolean forbidWrites(String tableName, TransactionId transactionId) throws IOException { return delegate.forbidWrites(tableName, transactionId); }
}
