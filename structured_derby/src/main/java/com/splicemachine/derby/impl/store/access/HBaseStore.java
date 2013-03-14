package com.splicemachine.derby.impl.store.access;



import java.util.Properties;

import com.splicemachine.constants.ITransactionManager;
import com.splicemachine.constants.ITransactionManagerFactory;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.txn.TransactionManagerFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.log4j.Logger;


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
		if (LOG.isTraceEnabled())
			LOG.trace("isReadOnly " + false);
		return false;
	}
	
	public TransactionInfo[] getTransactionInfo() {
		if (LOG.isTraceEnabled())
			LOG.trace("getTransactionInfo");
		return new TransactionInfo[0];
	}
	
	public void startReplicationMaster(String dbmaster, String host, int port,String replicationMode) {
		if (LOG.isTraceEnabled())
			LOG.trace("startReplication");
	}
	public void stopReplicationMaster() {
		if (LOG.isTraceEnabled())
			LOG.trace("stopReplicationMaster");
	}
	public void freeze() {
		if (LOG.isTraceEnabled())
			LOG.trace("freeze");		
	}
	public void unfreeze() {
		if (LOG.isTraceEnabled())
			LOG.trace("unfreeze");				
	}
	public void failover(String dbname) {
		if (LOG.isTraceEnabled())
			LOG.trace("failover");						
	}
	public void backup(String  backupDir, boolean wait) {
		if (LOG.isTraceEnabled())
			LOG.trace("backup");								
	}
	public void backupAndEnableLogArchiveMode(String backupDir,boolean deleteOnlineArchivedLogFiles,boolean wait) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("backupAndEnableLogArchiveMode");								
	}

	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("disableLogArchiveMode");								
	}

	public void checkpoint() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("checkpoint");									
	}

	public void waitForPostCommitToFinishWork() {
		if (LOG.isTraceEnabled())
			LOG.trace("waitForPostCommitToFinishWork");									
	}
	
	public void waitUntilQueueIsEmpty() {
		if (LOG.isTraceEnabled())
			LOG.trace("waitUntilQueueIsEmpty");									
	}
	public void getRawStoreProperties(TransactionController transactionController) {
		if (LOG.isTraceEnabled())
			LOG.trace("getRawStoreProperties " + transactionController);	
	}
	
	public Transaction findUserTransaction(ContextManager contextManager, String transactionName) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("findUserTransaction with Context Manager " + contextManager + " and transaction name " + transactionName);
		return transactionFactory.findUserTransaction(this, contextManager, transactionName);
	}

	public Transaction startGlobalTransaction(ContextManager contextManager, int format_id, byte[] global_id, byte[] branch_id) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startGlobalTransaction with Context Manager " + contextManager + " and format_id " + format_id + ", global_id " + global_id + ", branch_id + " + branch_id);
		return transactionFactory.startGlobalTransaction(this, contextManager, format_id, global_id, branch_id);
	}
	public boolean checkVersion(int requiredMajorVersion,int requiredMinorVersion,String feature) {
		if (LOG.isTraceEnabled())
			LOG.trace("checkVersion");											
		return true;
	}
	public Transaction startInternalTransaction(ContextManager contextManager) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startInternalTransaction with context manager " + contextManager);
		return transactionFactory.startInternalTransaction(this, contextManager);
	}

	public Transaction startNestedReadOnlyUserTransaction(CompatibilitySpace lockSpace, ContextManager contextManager,String nestedReadonlyUserTrans) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startNestedReadOnlyUserTransaction with context manager " + contextManager + ", lock space " + lockSpace + ", nestedReadonlyUserTrans " + nestedReadonlyUserTrans);											
		return transactionFactory.startNestedReadOnlyUserTransaction(this, lockSpace, contextManager, nestedReadonlyUserTrans);
	}

	public Transaction startNestedUpdateUserTransaction(ContextManager contextManager,String nestedUpdateUserTrans, boolean flush_log_on_xact_end) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startNestedUpdateUserTransaction with context manager " + contextManager + ", lock space " + nestedUpdateUserTrans + ", flush_log_on_xact_end " + flush_log_on_xact_end);											
		return transactionFactory.startNestedUpdateUserTransaction(this, contextManager, nestedUpdateUserTrans, flush_log_on_xact_end);
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
        ITransactionManagerFactory iTransactionManagerFactory;
        if (SpliceUtils.useSi) {
            iTransactionManagerFactory = new TransactionManagerFactory();
        } else {
            iTransactionManagerFactory = new ZkTransactionManagerFactory();
        }
        transactionFactory = new SpliceTransactionFactory(iTransactionManagerFactory);
		transactionFactory.boot(create, properties);
	}
	@Override
	public void stop() {
		if (LOG.isTraceEnabled())
			LOG.trace("stop ");													
	}
}
