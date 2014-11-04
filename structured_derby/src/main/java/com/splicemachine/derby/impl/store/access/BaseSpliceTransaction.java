package com.splicemachine.derby.impl.store.access;

import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.raw.*;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author Scott Fines
 * Date: 8/19/14
 */
public abstract class BaseSpliceTransaction implements Transaction {
    private static Logger LOG = Logger.getLogger(BaseSpliceTransaction.class);
    protected CompatibilitySpace compatibilitySpace;
    protected SpliceTransactionFactory spliceTransactionFactory;
    protected DataValueFactory dataValueFactory;
    protected SpliceTransactionContext transContext;
    protected String transName;

    protected volatile int	state;

    protected static final int	CLOSED		    = 0;
    protected static final int	IDLE		    = 1;
    protected static final int	ACTIVE		    = 2;

    public void setTransactionName(String s) {
        this.transName = s;
    }

    public String getTransactionName() {
        return this.transName;
    }

   	public LogInstant commitNoSync(int commitflag) throws StandardException {
				SpliceLogUtils.debug(LOG, "commitNoSync commitflag" + commitflag);
				return commit();
		}

    public void close() throws StandardException {
				SpliceLogUtils.debug(LOG,"close");

				if (transContext != null) {
						transContext.popMe();
						transContext = null;
				}
        clearState();
				state = CLOSED;
		}

    protected abstract void clearState();

    public void destroy() throws StandardException {
				SpliceLogUtils.debug(LOG,"destroy");
				if (state != CLOSED)
						abort();
				close();
		}

    @Override public DataValueFactory getDataValueFactory() throws StandardException { return dataValueFactory; }
    @Override public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException { return 0; }
    @Override public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException { return 0; }
    @Override public int rollbackToSavePoint(String name, Object kindOfSavepoint) throws StandardException { return 0; }
    @Override public ContainerHandle openContainer(ContainerKey containerId, int mode) throws StandardException { return null; }
    @Override public ContainerHandle openContainer(ContainerKey containerId, LockingPolicy locking, int mode) throws StandardException { return null; }
    @Override public long addContainer(long segmentId, long containerId, int mode, Properties tableProperties, int temporaryFlag) throws StandardException { return 0; }
    @Override public void dropContainer(ContainerKey containerId) throws StandardException {  }
    @Override public long addAndLoadStreamContainer(long segmentId, Properties tableProperties, RowSource rowSource) throws StandardException { return 0; }
    @Override public StreamContainerHandle openStreamContainer(long segmentId, long containerId, boolean hold) throws StandardException { return null; }
    @Override public void dropStreamContainer(long segmentId, long containerId) throws StandardException {  }
    @Override public void logAndDo(Loggable operation) throws StandardException {  }
    @Override public void addPostCommitWork(Serviceable work) {  }
    @Override public void addPostTerminationWork(Serviceable work) {  }
    @Override public FileResource getFileHandler() { return (spliceTransactionFactory == null ? null : spliceTransactionFactory.getFileHandler()); }
    @Override public boolean anyoneBlocked() { return false; }
    @Override public void createXATransactionFromLocalTransaction(int format_id, byte[] global_id, byte[] branch_id) throws StandardException {  }
    @Override public int xa_prepare() throws StandardException { return 0; }
    @Override public boolean isIdle() { return (state==IDLE); }
    @Override public boolean isPristine() { return (state == IDLE  ||  state == ACTIVE); }
    @Override public void xa_rollback() throws StandardException { abort(); }
    @Override public ContextManager getContextManager() { return transContext.getContextManager(); }
    @Override public CompatibilitySpace getCompatibilitySpace() { return compatibilitySpace; }
    @Override public void setNoLockWait(boolean noWait) {  }
    @Override public void setup(PersistentSet set) throws StandardException {  }
    @Override public GlobalTransactionId getGlobalId() { return null; }
    @Override public LockingPolicy getDefaultLockingPolicy() { return null; }
    @Override public LockingPolicy newLockingPolicy(int mode, int isolation, boolean stricterOk) { return null; }
    @Override public void setDefaultLockingPolicy(LockingPolicy policy) {  }

    @Override public void xa_commit(boolean onePhase) throws StandardException {
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

    public abstract TxnView getTxnInformation();

    public abstract void setActiveState(boolean nested, boolean additive, TxnView parentTxn,byte[] table);

    public abstract void setActiveState(boolean nested, boolean additive, TxnView parentTxn);

    public TxnView getActiveStateTxn(){
        setActiveState(false,false,null);
        return getTxnInformation();
    }
}
