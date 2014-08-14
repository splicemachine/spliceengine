package com.splicemachine.derby.impl.store.access;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
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
 * A view-only representation of a Derby transaction.
 *
 * This should be used when we created the transaction on a separate
 * node than where we are marshalling it.
 *
 * This implementation CANNOT be committed, so don't try to use it for
 * that purpose.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public class SpliceTransactionView implements Transaction {
    private static Logger LOG = Logger.getLogger(SpliceTransaction.class);
    protected CompatibilitySpace compatibilitySpace;
    protected DataValueFactory dataValueFactory;
    protected SpliceTransactionContext transContext;
    private String transName;

    private TxnView txn;

    protected volatile int	state;

    public SpliceTransactionView(CompatibilitySpace compatibilitySpace, DataValueFactory dataValueFactory,
                             String transName, TxnView txn) {
        SpliceLogUtils.trace(LOG, "Instantiating Splice transaction");
        this.compatibilitySpace = compatibilitySpace;
        this.dataValueFactory = dataValueFactory;
        this.transName = transName;
        this.state = SpliceTransaction.ACTIVE;
        this.txn = txn;
    }

    @Override public ContextManager getContextManager() { return transContext.getContextManager(); }
    @Override public CompatibilitySpace getCompatibilitySpace() { return compatibilitySpace; }

    /*no-op methods*/
    @Override public void setNoLockWait(boolean noWait) { }
    @Override public void setup(PersistentSet set) throws StandardException { }
    @Override public GlobalTransactionId getGlobalId() { return null; }
    @Override public LockingPolicy getDefaultLockingPolicy() { return null; }
    @Override public LockingPolicy newLockingPolicy(int mode, int isolation, boolean stricterOk) { return null; }
    @Override public void setDefaultLockingPolicy(LockingPolicy policy) {  }
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
    @Override public FileResource getFileHandler() { return null; }
    @Override public boolean anyoneBlocked() { return false; }
    @Override public void createXATransactionFromLocalTransaction(int format_id, byte[] global_id, byte[] branch_id) throws StandardException {  }

    @Override
    public LogInstant commit() throws StandardException {
        throw Exceptions.parseException(new CannotCommitException("Cannot commit from SpliceTransactionView"));
    }

    @Override
    public LogInstant commitNoSync(int commitflag) throws StandardException {
        return commit();
    }

    @Override
    public void abort() throws StandardException {
        throw new UnsupportedOperationException("Cannot abort from SpliceTransactionView");
    }

    @Override
    public void close() throws StandardException {
        if(transContext!=null){
            transContext.popMe();
            transContext = null;
        }
        txn = null;
        state = SpliceTransaction.CLOSED;
    }

    @Override
    public void destroy() throws StandardException {
        close();
    }

    @Override public boolean isIdle() { return state==SpliceTransaction.IDLE; }
    @Override public boolean isPristine() { return state ==SpliceTransaction.IDLE || state==SpliceTransaction.ACTIVE; }
    @Override public void xa_commit(boolean onePhase) throws StandardException { commit();  } //will throw error

    @Override public int xa_prepare() throws StandardException { return 0; }

    //will throw exception
    @Override public void xa_rollback() throws StandardException { abort(); }

    @Override
    public String getActiveStateTxIdString() {
        if(txn!=null)
            return txn.toString();
        else
            return null;
    }

    @Override public DataValueFactory getDataValueFactory() throws StandardException { return dataValueFactory; }
}
