package com.splicemachine.derby.impl.store.access;

import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.GlobalTransactionId;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class SpliceTransaction implements Transaction {
	private static Logger LOG = Logger.getLogger(SpliceTransaction.class);
	protected CompatibilitySpace compatibilitySpace;
	protected DataValueFactory dataValueFactory;
	protected SpliceTransactionContext transContext;
	private TransactionId transactionId;
	private String transName;
	
	protected volatile int	state;
	
	protected static final int	CLOSED		    = 0;
	protected static final int	IDLE		    = 1;
	protected static final int	ACTIVE		    = 2;

	//FIXME: this is a temp workaround to integrate our existing transaction code. We need to implement the function here eventually.
	protected TransactorControl transactor;

	public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             DataValueFactory dataValueFactory,
                             TransactorControl transactor,
                             String transName) {
		SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
		this.compatibilitySpace = compatibilitySpace;
		this.dataValueFactory = dataValueFactory;
		this.transactor = transactor;
		this.transName = transName;
		this.state = IDLE;
	}

	public SpliceTransaction(CompatibilitySpace compatibilitySpace, DataValueFactory dataValueFactory,
                             TransactorControl transactor,
                             String transName, TransactionId transactionId) {
			SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
			this.compatibilitySpace = compatibilitySpace;
			this.dataValueFactory = dataValueFactory;
			this.transactor = transactor;
			this.transName = transName;
			this.state = ACTIVE;
			this.transactionId = transactionId;
}

	public ContextManager getContextManager() {
		SpliceLogUtils.debug(LOG,"getContextManager");
		return transContext.getContextManager();
	}
	
	public CompatibilitySpace getCompatibilitySpace() {
		SpliceLogUtils.debug(LOG,"getCompatibilitySpace");
		return compatibilitySpace;
	}
	
	public TransactionId getTransactionId() {
		return this.transactionId;
	}
	
	public void setTransactionState(TransactionId ts) {
		this.transactionId = ts;
		this.state = ACTIVE;
	}

	public void setTransactionName(String s) {
		this.transName = s;
	}
	
	public String getTransactionName() {
		return this.transName;
	}
	
	public void setNoLockWait(boolean noWait) {
		SpliceLogUtils.debug(LOG,"setNoLockWait " + noWait);
	}

	public void setup(PersistentSet set) throws StandardException {
		SpliceLogUtils.debug(LOG,"setup " + set);
		
	}

	public GlobalTransactionId getGlobalId() {
		SpliceLogUtils.debug(LOG,"getGlobalId");
		return null;
	}

	public LockingPolicy getDefaultLockingPolicy() {
		SpliceLogUtils.debug(LOG,"getDefaultLockingPolicy");
		return null;
	}
	
	public LockingPolicy newLockingPolicy(int mode, int isolation,boolean stricterOk) {
		SpliceLogUtils.debug(LOG,"newLockingPolicy mode " + mode + ", isolation "+ isolation + ", " + stricterOk);
		return null;
	}

	public void setDefaultLockingPolicy(LockingPolicy policy) {
		SpliceLogUtils.debug(LOG,"setDefaultLockingPolicy policy " + policy);
	}

	public LogInstant commit() throws StandardException {
		SpliceLogUtils.debug(LOG, "commit, state=" + state + " for transaction " + transactionId.getTransactionIdString());
		
		if (state == IDLE) {
			SpliceLogUtils.debug(LOG, "The transaction is in idle state and there is nothing to commit, transID=" + transactionId.getTransactionIdString());
			return null;
		}
		
		if (state == CLOSED)
        {
			throw StandardException.newException("Transaction has already closed and cannot commit again");
        }
			
		try {
			transactor.commit(this.transactionId);
			state = IDLE;
		} catch (Exception e) {
			throw StandardException.newException(e.getMessage(), e);
		}
		return null;
	}

	public LogInstant commitNoSync(int commitflag) throws StandardException {
		SpliceLogUtils.debug(LOG,"commitNoSync commitflag" + commitflag);
		return commit();
	}

	public void abort() throws StandardException {
		SpliceLogUtils.debug(LOG,"abort");	
		try {
			if (state == CLOSED)
				return;
            transactor.rollback(this.transactionId);
			state = IDLE;
		} catch (Exception e) {
			throw StandardException.newException(e.getMessage(), e);
		}
		
	}
	
	public void close() throws StandardException {
		SpliceLogUtils.debug(LOG,"close");	

		transContext.popMe();
		transContext = null;
		transactionId = null;
        transactor = null;
		state = CLOSED;
	}

	public void destroy() throws StandardException {
		SpliceLogUtils.debug(LOG,"destroy");
		if (state != CLOSED)
            abort();
		close();
	}
	
	public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException {
		SpliceLogUtils.debug(LOG,"setSavePoint name " + name + ", kindOfSavepoint " + kindOfSavepoint);
		return 0;
	}

	public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException {
		SpliceLogUtils.debug(LOG,"releaseSavePoint name " + name + ", kindOfSavepoint " + kindOfSavepoint);
		return 0;
	}

	public int rollbackToSavePoint(String name, Object kindOfSavepoint) throws StandardException {
		SpliceLogUtils.debug(LOG,"rollbackToSavePoint name " + name + ", kindOfSavepoint " + kindOfSavepoint);
		return 0;
	}

	public ContainerHandle openContainer(ContainerKey containerId, int mode) throws StandardException {
		SpliceLogUtils.debug(LOG,"openContainer");
		return null;
	}
	
	public ContainerHandle openContainer(ContainerKey containerId, LockingPolicy locking, int mode) throws StandardException {
		SpliceLogUtils.debug(LOG,"openContainer");
		return null;
	}

	public long addContainer(long segmentId, long containerId, int mode,Properties tableProperties, int temporaryFlag) throws StandardException {
		SpliceLogUtils.debug(LOG,"addContainer");
		return 0;
	}

	public void dropContainer(ContainerKey containerId) throws StandardException {
		SpliceLogUtils.debug(LOG,"dropContainer");
	}

	public long addAndLoadStreamContainer(long segmentId,Properties tableProperties, RowSource rowSource) throws StandardException {
		SpliceLogUtils.debug(LOG,"addAndLoadStreamContainer");
		return 0;
	}

	public StreamContainerHandle openStreamContainer(long segmentId,long containerId, boolean hold) throws StandardException {
		SpliceLogUtils.debug(LOG,"openStreamContainer");
		return null;
	}

	public void dropStreamContainer(long segmentId, long containerId) throws StandardException {
		SpliceLogUtils.debug(LOG,"dropStreamContainer");		
	}

	public void logAndDo(Loggable operation) throws StandardException {
		SpliceLogUtils.debug(LOG,"logAndDo operation " + operation);				
	}

	public void addPostCommitWork(Serviceable work) {
		SpliceLogUtils.debug(LOG,"addPostCommitWork work " + work);						
	}

	public void addPostTerminationWork(Serviceable work) {
		SpliceLogUtils.debug(LOG,"addPostCommitWork work " + work);								
	}

	public boolean isIdle() {
		SpliceLogUtils.debug(LOG, "isIdle state=" + state + " for transaction " + transactionId.getTransactionIdString());
		return (state==IDLE);
	}

	public boolean isPristine() {
		SpliceLogUtils.debug(LOG,"isPristine");						
		return (state == IDLE  ||  state == ACTIVE);
	}

	public FileResource getFileHandler() {
		SpliceLogUtils.debug(LOG,"getFileHandler");						
		return null;
	}

	public boolean anyoneBlocked() {
		SpliceLogUtils.debug(LOG,"anyoneBlocked");						
		//return getLockFactory().anyoneBlocked();
		return false;
	}
	
	public void createXATransactionFromLocalTransaction(int format_id,byte[] global_id, byte[] branch_id) throws StandardException {
		SpliceLogUtils.debug(LOG,"createXATransactionFromLocalTransaction");								
	}

	public void xa_commit(boolean onePhase) throws StandardException {
		SpliceLogUtils.debug(LOG,"xa_commit");	
		try {
			if (onePhase)
				commit();
			else {
				xa_prepare();
				commit();
			}
		} catch (Exception e) {
			throw StandardException.newException(e.getMessage(), e);
		}	
	}

	public int xa_prepare() throws StandardException {
		SpliceLogUtils.debug(LOG,"xa_prepare");
		return 0;
	}

	public void xa_rollback() throws StandardException {
		SpliceLogUtils.debug(LOG,"xa_rollback");	
		abort();
	}
	
	public String getActiveStateTxIdString() {
		SpliceLogUtils.debug(LOG,"getActiveStateTxIdString");
		setActiveState(false, false, false, null);
		if (transactionId !=null)
			return transactionId.getTransactionIdString();
		else
			return null;
	}

	public DataValueFactory getDataValueFactory() throws StandardException {
		SpliceLogUtils.debug(LOG,"getDataValueFactory");
		return dataValueFactory;
	}

	public final String getContextId() {
        SpliceTransactionContext tempxc = transContext;
        return (tempxc == null) ? null : tempxc.getIdName();
    }	
	
	public final void setActiveState(boolean readOnly, boolean nested, boolean dependent, String parentTransactionID) {
		if (state == IDLE)
		{
			try {
				synchronized(this)
				{
                    boolean allowWrites = !readOnly;
                    this.setTransactionState(generateTransactionId(nested, dependent, parentTransactionID, allowWrites));
					state = ACTIVE;
					//justCreated = false;
				}
			} catch (Exception e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}
	}

    private TransactionId generateTransactionId(boolean nested, boolean dependent, String parentTransactionID, boolean allowWrites) throws IOException {
        TransactionId result;
        if (nested) {
            final TransactionId parentTransaction = transactor.transactionIdFromString(parentTransactionID);
            if (dependent) {
                result = transactor.beginChildTransaction(parentTransaction, dependent, allowWrites);
            } else {
                result = transactor.beginChildTransaction(parentTransaction, dependent, allowWrites, true, true);
            }
        } else {
            result = transactor.beginTransaction();
        }
        return result;
    }

	public int getTransactionStatus() {
		return state;
	}

}
