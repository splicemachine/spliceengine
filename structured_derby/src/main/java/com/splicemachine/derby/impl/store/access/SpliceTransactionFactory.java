package com.splicemachine.derby.impl.store.access;

import com.splicemachine.SpliceConfiguration;
import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.HbaseConfigurationSource;
import com.splicemachine.si.api.TransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Properties;

public class SpliceTransactionFactory implements ModuleControl, ModuleSupportable{
	private static Logger LOG = Logger.getLogger(SpliceTransactionFactory.class);
	
	protected static final String USER_CONTEXT_ID = "UserTransaction";
	protected static final String NESTED_READONLY_USER_CONTEXT_ID = "NestedRawReadOnlyUserTransaction";
	protected static final String NESTED_UPDATE_USER_CONTEXT_ID = "NestedRawUpdateUserTransaction";
	protected static final String INTERNAL_CONTEXT_ID    = "InternalTransaction";
	protected static final String NTT_CONTEXT_ID         = "NestedTransaction";
	
	protected SpliceLockFactory lockFactory;
	protected J2SEDataValueFactory dataValueFactory;
	protected ContextService contextFactory;
	protected HBaseStore hbaseStore;
    private TransactorFactory transactorFactory;

    public SpliceTransactionFactory(TransactorFactory transactorFactory) {
        this.transactorFactory = transactorFactory;
    }

    public StandardException markCorrupt(StandardException originalError) {
		return null;
	}

	public LockFactory getLockFactory() {
		return lockFactory;
	}

	public Object getXAResourceManager() throws StandardException {
		return null;
	}

	public Transaction marshalTransaction(HBaseStore hbaseStore, ContextManager contextMgr, String transName, String transactionID) throws StandardException {
		try {
			HbaseConfigurationSource configSource = new HbaseConfigurationSource() {
               @Override
               public Configuration getConfiguration() {
                   return SpliceConfiguration.create();
               }
           };
           Transactor transactor = transactorFactory.newTransactor(configSource);
           Transaction trans = new SpliceTransaction(new SpliceLockSpace(),dataValueFactory,transactor,transName, transactor.transactionIdFromString(transactionID));
           return trans;
		} catch (Exception e) {
			throw SpliceStandardLogUtils.logAndReturnStandardException(LOG, "marshallTransactionFailure", e);
		}
	}

	
	public Transaction startTransaction(HBaseStore hbaseStore, ContextManager contextMgr, String transName) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, transName, false, USER_CONTEXT_ID, false, false, null);
	}

	public Transaction startNestedReadOnlyUserTransaction(HBaseStore hbaseStore, CompatibilitySpace compatibilitySpace,ContextManager contextMgr, String transName, String parentTransactionId) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startNestedReadOnlyUserTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startNestedReadOnlyUserTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, true, null, false, NESTED_READONLY_USER_CONTEXT_ID, true, false, parentTransactionId);
	}

	public Transaction startNestedUpdateUserTransaction(HBaseStore hbaseStore,ContextManager contextMgr, String transName,boolean flush_log_on_xact_end, String parentTransactionid) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startNestedUpdateUserTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startNestedUpdateUserTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, transName, false, NESTED_UPDATE_USER_CONTEXT_ID, true, false, parentTransactionid);
	}

	public Transaction startGlobalTransaction(HBaseStore hbaseStore,ContextManager contextMgr, int format_id, byte[] global_id,byte[] branch_id) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startGlobalTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startGlobalTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, null, false, USER_CONTEXT_ID, false, false, null);
	}

	public Transaction findUserTransaction(HBaseStore hbaseStore,ContextManager contextMgr, String transName) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("findUserTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("findUserTransaction, passed in context mgr not the same as current context mgr");
		}
		SpliceTransactionContext tc = (SpliceTransactionContext)contextMgr.getContext(USER_CONTEXT_ID);
		if (tc == null) {
			SpliceLogUtils.debug(LOG, "findUserTransaction, transaction controller is null for UserTransaction");
			return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, transName, false, USER_CONTEXT_ID, false, false, null);
		} else {
			SpliceLogUtils.debug(LOG,"findUserTransaction, transaction controller is NOT null for UserTransaction");
			return tc.getTransaction();
		}
	}

	public Transaction startNestedTopTransaction(HBaseStore hbaseStore, ContextManager contextMgr) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startNestedTopTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startNestedTopTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, null, true, NTT_CONTEXT_ID, false, false, null);
	}

	public Transaction startInternalTransaction(HBaseStore hbaseStore, ContextManager contextMgr) throws StandardException {
		if (contextMgr != contextFactory.getCurrentContextManager()) 
			LOG.error("##############startInternalTransaction, passed in context mgr not the same as current context mgr");
		if (this.hbaseStore == hbaseStore) {
			LOG.error("##############startInternalTransaction, passed in context mgr not the same as current context mgr");
		}
		return startCommonTransaction(hbaseStore, contextMgr, lockFactory, dataValueFactory, false, null, true, INTERNAL_CONTEXT_ID, false, false, null);
	}

	private SpliceTransaction startCommonTransaction(HBaseStore hbaseStore,
                                                     ContextManager contextMgr, SpliceLockFactory lockFactory, J2SEDataValueFactory dataValueFactory,
                                                     boolean readOnly, String transName, boolean abortAll, String contextName, boolean nested, boolean dependent, String parentTransactionID) {
        try {
            HbaseConfigurationSource configSource = new HbaseConfigurationSource() {
                @Override
                public Configuration getConfiguration() {
                    return SpliceConfiguration.create();
                }
            };
            Transactor transactor = transactorFactory.newTransactor(configSource);
			SpliceTransaction trans = new SpliceTransaction(new SpliceLockSpace(), dataValueFactory, transactor, transName); 
			trans.setTransactionName(transName);
			
			SpliceTransactionContext context = new SpliceTransactionContext(contextMgr, contextName, trans, abortAll, hbaseStore);
			
			trans.setActiveState(readOnly, nested, dependent, parentTransactionID);
			SpliceLogUtils.debug(LOG, "transaction type="+context.getIdName()+",transactionID="+trans.getTransactionId().getTransactionIdString());
			return trans;
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
        return null;
	}
	
	public boolean findTransaction(TransactionId id, Transaction tran) {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - findTransaction trans="+tran.toString()+",id="+id.toString());
		return false;
	}

	public void resetTranId() throws StandardException {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - resetTranId");
	}

	public LogInstant firstUpdateInstant() {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - firstUpdateInstant");
		return null;
	}

	public void handlePreparedXacts(HBaseStore hbaseStore)
			throws StandardException {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - handlePreparedXacts");
	}

	public void rollbackAllTransactions(Transaction recoveryTransaction, HBaseStore hbaseStore) throws StandardException {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - rollbackAllTransactions trnas="+recoveryTransaction.getGlobalId());
	}

	public boolean submitPostCommitWork(Serviceable work) {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - submitPostCommitWork");
		return false;
	}

	public void setHBaseStoreFactory(HBaseStore hbaseStore) throws StandardException {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - setHBaseStoreFactory");
		this.hbaseStore = hbaseStore;
	}

	public boolean noActiveUpdateTransaction() {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - noActiveUpdateTransaction");
		return false;
	}

	public boolean hasPreparedXact() {
		// TODO Auto-generated method stub
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - hasPreparedXact");
		return false;
	}

	public void createFinished() throws StandardException {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory - createFinished");
	}

	public Formatable getTransactionTable() {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -getTransactionTable");
		return null;
	}

	public void useTransactionTable(Formatable transactionTable)
			throws StandardException {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -useTransactionTable");
	}

	public TransactionInfo[] getTransactionInfo() {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -getTransactionInfo");
		return null;
	}

	public boolean blockBackupBlockingOperations(boolean wait) throws StandardException {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -blockBackupBlockingOperations");
		return false;
	}

	public void unblockBackupBlockingOperations() {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -unblockBackupBlockingOperations");
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
	}

	public void stop() {
		SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -stop");
	}

}
