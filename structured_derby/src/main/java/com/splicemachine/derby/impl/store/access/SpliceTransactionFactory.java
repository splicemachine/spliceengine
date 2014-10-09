package com.splicemachine.derby.impl.store.access;

import com.splicemachine.derby.impl.store.access.base.SpliceLocalFileResource;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.impl.io.DirStorageFactory4;
import org.apache.derby.io.StorageFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class SpliceTransactionFactory implements ModuleControl, ModuleSupportable{
		private static Logger LOG = Logger.getLogger(SpliceTransactionFactory.class);

		protected static final String USER_CONTEXT_ID = "UserTransaction";
		protected static final String NESTED_READONLY_USER_CONTEXT_ID = "NestedRawReadOnlyUserTransaction";

		protected SpliceLockFactory lockFactory;
		protected J2SEDataValueFactory dataValueFactory;
		protected ContextService contextFactory;
		protected HBaseStore hbaseStore;
        protected StorageFactory storageFactory;
        protected FileResource fileHandler;

		public LockFactory getLockFactory() {
				return lockFactory;
		}

		public Object getXAResourceManager() throws StandardException {
				return null;
		}

		/**
		 * place a transaction which was created outside of derby (e.g. across a serialization boundary, or a
		 * manually-created transaction) within the context of Derby, so that derby system calls can be used.
		 *
		 * This is relatively cheap--it creates a new object, but registers the existing transaction, rather than
		 * creating a new one.
		 *
		 * @param transName the name of the transaction
		 * @param txn the transaction to marshall
		 * @return a derby representation of the transaction
		 * @throws StandardException if something goes wrong (which it isn't super likely to do)
		 */
		public Transaction marshalTransaction(String transName, TxnView txn) throws StandardException {
				try {
						return new SpliceTransactionView(NoLockSpace.INSTANCE, this, dataValueFactory, transName, txn);
				} catch (Exception e) {
						SpliceLogUtils.logAndThrow(LOG,"marshallTransaction failure", Exceptions.parseException(e));
						return null; // can't happen
				}
		}

		private void checkContextAndStore(HBaseStore hbaseStore, ContextManager contextMgr, String methodName) {
			if (contextMgr != contextFactory.getCurrentContextManager()) 
				LOG.error(methodName + ": passed in context mgr not the same as current context mgr");
			if (this.hbaseStore != hbaseStore) {
          SpliceLogUtils.debug(LOG,"%s: passed in hbase store not the same as current context's store",methodName);
			}
		}
		
		/**
		 * Starts a new transaction with the given name.
		 *
		 * This will create a new top-level transaction. As such, it will likely be an expensive network operation.
		 *
		 * @param hbaseStore the hbase store relevant to the transaction
		 * @param contextMgr the context manager
		 * @param transName the name of the transaction
		 * @return a new transaction with the specified name and context.
		 * @throws StandardException if something goes wrong creating the transaction
		 */
		public Transaction startTransaction(HBaseStore hbaseStore, ContextManager contextMgr, String transName) throws StandardException {
			checkContextAndStore(hbaseStore, contextMgr, "startTransaction");
			
			return startCommonTransaction(hbaseStore, contextMgr,  dataValueFactory, transName, false, USER_CONTEXT_ID);
		}

		/**
		 * Start a "nested" transaction.
		 *
		 * Derby's nested transaction is functionally equivalent to Splice's child transaction, and this method
		 * will actually create a child transaction.
		 *
		 * @param hbaseStore the hbase store relevant to the transaction
		 * @param contextMgr the context manager
		 * @param parentTxn the parent transaction
		 * @return a new child transaction of the parent transaction
		 * @throws StandardException if something goes wrong
		 */
		public Transaction startNestedTransaction(HBaseStore hbaseStore,
												  ContextManager contextMgr,
												  Txn parentTxn) throws StandardException {
			checkContextAndStore(hbaseStore, contextMgr, "startNestedTransaction");
			
			return startNestedTransaction(hbaseStore, contextMgr,
				dataValueFactory, null, false, NESTED_READONLY_USER_CONTEXT_ID, false, parentTxn);
		}

		/**
		 * Find or create a user-level transaction.
		 *
		 * If the current context already has a created user level transaction, then it will return the value
		 * which currently exists. Otherwise, it will create a new user-level transaction.
		 *
		 * Because of the possibility of creating a new transaction, it may use a network call, which makes this
		 * a potentially expensive call.
		 *
		 * @param hbaseStore the hbase store relevant to the transaction
		 * @param contextMgr the context manager
		 * @param transName the name of the transaction
		 * @return a new user-level transaction
		 * @throws StandardException if something goes wrong
		 */
		public Transaction findUserTransaction(HBaseStore hbaseStore,
											   ContextManager contextMgr,
											   String transName) throws StandardException {
				checkContextAndStore(hbaseStore, contextMgr, "findUserTransaction");
			
				SpliceTransactionContext tc = (SpliceTransactionContext)contextMgr.getContext(USER_CONTEXT_ID);
				if (tc == null) {
						//We don't have a transaction yet, so create a new top-level transaction. This may require a network call
						SpliceLogUtils.trace(LOG, "findUserTransaction, transaction controller is null for UserTransaction");
						return startCommonTransaction(hbaseStore, contextMgr, dataValueFactory,transName, false, USER_CONTEXT_ID);
				} else {
						//we already have a transaction, so just return that one
						SpliceLogUtils.trace(LOG,"findUserTransaction, transaction controller is NOT null for UserTransaction");
						return tc.getTransaction();
				}
		}

    protected final SpliceTransaction startNestedTransaction(HBaseStore hbaseStore,
                                                             ContextManager contextMgr,
                                                             J2SEDataValueFactory dataValueFactory,
                                                             String transName,
                                                             boolean abortAll,
                                                             String contextName,
                                                             boolean additive,
                                                             Txn parentTxn) {
        try {
            TxnLifecycleManager lifecycleManager = TransactionLifecycle.getLifecycleManager();
						/*
						 * All transactions start as read only.
						 *
						 * If parentTxn!=null, then this will create a read-only child transaction (which is essentially
						 * a duplicate of the parent transaction); this requires no network calls immediately, but will require
						 * 2 network calls when elevateTransaction() is called.
						 *
						 * if parentTxn==null, then this will make a call to the timestamp source to generate a begin timestamp
						 * for a read-only transaction; this requires a single network call.
						 */
            Txn txn = lifecycleManager.beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, additive,null);
            SpliceTransaction trans = new SpliceTransaction(NoLockSpace.INSTANCE, this, dataValueFactory, transName,txn);
            trans.setTransactionName(transName);

            SpliceTransactionContext context = new SpliceTransactionContext(contextMgr, contextName, trans, abortAll, hbaseStore);

            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "transaction type=%s,%s",context.getIdName(),txn);

            return trans;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

		/**
		 * Start a new transaction.
		 *
		 * If parentTxn==null, then this will create a new top-level transaction (e.g. a child of the ROOT_TRANSACTION).
		 * Performing this generally requires a network call, and so is expensive.
		 *
		 * If parentTxn!=null, then this may create a "read-only child transaction"; this is in effect a copy of the
		 * parent transaction's information along with some additional logic for when elevation occur. Therefore, this
		 * way is likely very inexpensive to call this method, but it will be doubly expensive when elevateTransaction()
		 * is called (as it will require 2 network calls to elevate).
		 *
		 * @see com.splicemachine.si.api.TxnLifecycleManager#beginChildTransaction(com.splicemachine.si.api.TxnView, com.splicemachine.si.api.Txn.IsolationLevel, boolean, byte[])
		 */
		protected final SpliceTransaction startCommonTransaction(HBaseStore hbaseStore,
																										 ContextManager contextMgr,
																										 J2SEDataValueFactory dataValueFactory,
																										 String transName,
																										 boolean abortAll,
																										 String contextName) {
        SpliceTransaction trans = new SpliceTransaction(NoLockSpace.INSTANCE,this,dataValueFactory,transName);
        trans.setTransactionName(transName);

        SpliceTransactionContext context = new SpliceTransactionContext(contextMgr, contextName, trans, abortAll, hbaseStore);

        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "transaction type=%s,%s",context.getIdName(),transName);

        return trans;
		}

		@Override
		public boolean canSupport(Properties properties) {
				SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -canSupport");
				return true;
		}

		public void boot(boolean create, Properties properties) throws StandardException {
				dataValueFactory = new J2SEDataValueFactory();
				dataValueFactory.boot(create, properties);
				contextFactory = ContextService.getFactory();
				lockFactory = new SpliceLockFactory();
				lockFactory.boot(create, properties);
        storageFactory = new DirStorageFactory4();
        try {
            storageFactory.init(null, "splicedb", null, null);  // Grumble, grumble, ... not a big fan of hard coding "splicedb" as the dbname.
        } catch (IOException ioe) {
            throw StandardException.newException(
                    SQLState.FILE_UNEXPECTED_EXCEPTION, ioe);
        }
        fileHandler = new SpliceLocalFileResource(storageFactory);
    }

		public void stop() {
				SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -stop");
		}

	public FileResource getFileHandler() {			
		return fileHandler;
	}
}
