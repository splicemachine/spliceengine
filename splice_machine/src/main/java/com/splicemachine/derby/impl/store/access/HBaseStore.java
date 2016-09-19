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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.TransactionInfo;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.Properties;

public class HBaseStore implements ModuleControl, ModuleSupportable {
	protected SpliceTransactionFactory transactionFactory;
	private static Logger LOG = Logger.getLogger(HBaseStore.class);
	public HBaseStore() {
		
	}
	public void createFinished() {
		if (LOG.isTraceEnabled())
			LOG.trace("createFinished");
	}
	
	public LockFactory getLockFactory() {
		if (LOG.isTraceEnabled())
			LOG.trace("getLockFactory");
		return transactionFactory.getLockFactory();
	}
	
	public Object getXAResourceManager() {
		if (LOG.isTraceEnabled())
			LOG.trace("getXAResourceManager");
		try {
			return (Object) transactionFactory.getXAResourceManager();
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public boolean isReadOnly() {
		SpliceLogUtils.trace(LOG,"isReadOnly %s",false);
		return false;
	}
	
	public TransactionInfo[] getTransactionInfo() {
		SpliceLogUtils.trace(LOG,"getTransactionInfo");
		return new TransactionInfo[0];
	}
	
	public void startReplicationMaster(String dbmaster, String host, int port,String replicationMode) {
		SpliceLogUtils.trace(LOG,"startReplication");
	}
	public void stopReplicationMaster() {
		SpliceLogUtils.trace(LOG,"stopReplicationMaster");
	}
	public void freeze() {
		SpliceLogUtils.trace(LOG,"freeze");		
	}
	public void unfreeze() {
		SpliceLogUtils.trace(LOG,"unfreeze");				
	}
	public void failover(String dbname) {
		SpliceLogUtils.trace(LOG,"failover");						
	}
	public void backup(String  backupDir, boolean wait) {
		SpliceLogUtils.trace(LOG,"backup");								
	}
	public void backupAndEnableLogArchiveMode(String backupDir,boolean deleteOnlineArchivedLogFiles,boolean wait) throws StandardException {
		SpliceLogUtils.trace(LOG,"backupAndEnableLogArchiveMode");								
	}

	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles) throws StandardException {
		SpliceLogUtils.trace(LOG,"disableLogArchiveMode");								
	}

	public void checkpoint() throws StandardException {
		SpliceLogUtils.trace(LOG,"checkpoint");									
	}

		public void waitUntilQueueIsEmpty() {
				SpliceLogUtils.trace(LOG,"waitUntilQueueIsEmpty");
		}
		public void getRawStoreProperties(TransactionController transactionController) {
				SpliceLogUtils.trace(LOG,"getRawStoreProperties %s",transactionController);
		}

		public Transaction marshallTransaction(ContextManager contextManager, String transactionName, TxnView txn) throws StandardException {
				SpliceLogUtils.trace(LOG, "marshalTransaction with Context Manager %s  and transaction name %s", contextManager, transactionName);
				return transactionFactory.marshalTransaction(transactionName, txn);
		}

		/**
		 * Finds or creates a new user-level transaction. If the Context manager already has a user-level transaction
		 * available, then this will return that one; otherwise, a new user-level transaction is created. This therefore
		 * has a minimum of 1 network call, and is therefore an expensive operation.
		 */
		public Transaction findUserTransaction(ContextManager contextManager, String transactionName) throws StandardException {
				SpliceLogUtils.trace(LOG, "marshalTransaction with Context Manager %s  and transaction name %s", contextManager, transactionName);
				return transactionFactory.findUserTransaction(this, contextManager, transactionName);
		}

		/**
		 * Start a "global transaction". In this case, it delegates to just creating a new top-level transaction.
		 *
		 * @param contextManager the context manager to use
		 * @param format_id
		 * @param global_id
		 * @param branch_id
		 * @return
		 * @throws StandardException
		 */
		public Transaction startGlobalTransaction(ContextManager contextManager, int format_id, byte[] global_id, byte[] branch_id) throws StandardException {
				SpliceLogUtils.trace(LOG,"startGlobalTransaction with ContextManager %s and format_id %d, global_id %s, branch_id %s",contextManager,format_id,global_id,branch_id);
				return transactionFactory.startTransaction(this, contextManager, null);
		}

		public boolean checkVersion(int requiredMajorVersion,int requiredMinorVersion,String feature) {
				if (LOG.isTraceEnabled())
						LOG.trace("checkVersion");
				return true;
		}

	public Transaction startNestedTransaction(CompatibilitySpace lockSpace,
											  ContextManager contextManager,
											  String nestedReadonlyUserTrans,TxnView parentTxn) throws StandardException{
		if(LOG.isTraceEnabled())
			LOG.trace("startNestedReadOnlyUserTransaction with context manager "+contextManager+", lock space "+lockSpace+", nestedReadonlyUserTrans "+nestedReadonlyUserTrans);
		return transactionFactory.startNestedTransaction(this,contextManager,parentTxn);
	}

		@Override
		public boolean canSupport(Properties properties) {
				if (LOG.isTraceEnabled())
						LOG.trace("canSupport with properties " + properties);
				return true;
		}
		@Override
		public void boot(boolean create, Properties properties) throws StandardException {
				if (LOG.isTraceEnabled())
						LOG.trace("boot with properties " + properties);
				transactionFactory = new SpliceTransactionFactory();
				transactionFactory.boot(create, properties);
		}
		@Override
		public void stop() {
				if (LOG.isTraceEnabled())
						LOG.trace("stop ");
		}
}
