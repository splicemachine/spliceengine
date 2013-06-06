package com.splicemachine.derby.impl.store.access;

import java.util.Properties;

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

import com.splicemachine.utils.SpliceLogUtils;

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

	public void waitForPostCommitToFinishWork() {
		SpliceLogUtils.trace(LOG,"waitForPostCommitToFinishWork");									
	}
	
	public void waitUntilQueueIsEmpty() {
		SpliceLogUtils.trace(LOG,"waitUntilQueueIsEmpty");									
	}
	public void getRawStoreProperties(TransactionController transactionController) {
		SpliceLogUtils.trace(LOG,"getRawStoreProperties %s",transactionController);	
	}

	public Transaction marshallTransaction(ContextManager contextManager, String transactionName, String transactionId) throws StandardException {
		SpliceLogUtils.trace(LOG, "marshalTransaction with Context Manager %s  and transaction name %s", contextManager, transactionName);
		return transactionFactory.marshalTransaction(this, contextManager, transactionName, transactionId);
	}
	
	public Transaction findUserTransaction(ContextManager contextManager, String transactionName) throws StandardException {
		SpliceLogUtils.trace(LOG, "marshalTransaction with Context Manager %s  and transaction name %s", contextManager, transactionName);
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

	public Transaction startNestedReadOnlyUserTransaction(CompatibilitySpace lockSpace, ContextManager contextManager, String nestedReadonlyUserTrans, String parentTransactionId) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startNestedReadOnlyUserTransaction with context manager " + contextManager + ", lock space " + lockSpace + ", nestedReadonlyUserTrans " + nestedReadonlyUserTrans);											
		return transactionFactory.startNestedReadOnlyUserTransaction(this, lockSpace, contextManager, nestedReadonlyUserTrans, parentTransactionId);
	}

	public Transaction startNestedUpdateUserTransaction(ContextManager contextManager, String nestedUpdateUserTrans, boolean flush_log_on_xact_end, String parentTransactionId) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("startNestedUpdateUserTransaction with context manager " + contextManager + ", lock space " + nestedUpdateUserTrans + ", flush_log_on_xact_end " + flush_log_on_xact_end);											
		return transactionFactory.startNestedUpdateUserTransaction(this, contextManager, nestedUpdateUserTrans, flush_log_on_xact_end, parentTransactionId);
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
