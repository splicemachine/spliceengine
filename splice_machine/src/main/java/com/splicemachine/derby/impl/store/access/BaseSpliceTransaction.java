package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.property.PersistentSet;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.raw.*;
import com.splicemachine.db.iapi.store.raw.log.LogInstant;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

/**
 * @author Scott Fines
 *         Date: 8/19/14
 */
public abstract class BaseSpliceTransaction implements Transaction{
    private static Logger LOG=Logger.getLogger(BaseSpliceTransaction.class);
    protected CompatibilitySpace compatibilitySpace;
    protected SpliceTransactionFactory spliceTransactionFactory;
    protected DataValueFactory dataValueFactory;
    protected SpliceTransactionContext transContext;
    protected String transName;

    protected volatile int state;

    protected static final int CLOSED=0;
    protected static final int IDLE=1;
    protected static final int ACTIVE=2;

    public void setTransactionName(String s){
        this.transName=s;
    }

    public String getTransactionName(){
        return this.transName;
    }

    public LogInstant commitNoSync(int commitflag) throws StandardException{
        SpliceLogUtils.debug(LOG,"commitNoSync commitflag"+commitflag);
        return commit();
    }

    public void close() throws StandardException{
        SpliceLogUtils.debug(LOG,"close");

        if(transContext!=null){
            transContext.popMe();
            transContext=null;
        }
        clearState();
        state=CLOSED;
    }

    public abstract boolean allowsWrites();

    protected abstract void clearState();

    public void destroy() throws StandardException{
        SpliceLogUtils.debug(LOG,"destroy");
        if(state!=CLOSED)
            abort();
        close();
    }

    @Override
    public DataValueFactory getDataValueFactory() throws StandardException{
        return dataValueFactory;
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        return 0;
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        return 0;
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        return 0;
    }

    @Override
    public void addPostCommitWork(Serviceable work){
    }

    @Override
    public void addPostTerminationWork(Serviceable work){
    }

    @Override
    public FileResource getFileHandler(){
        return (spliceTransactionFactory==null?null:spliceTransactionFactory.getFileHandler());
    }

    @Override
    public boolean anyoneBlocked(){
        return false;
    }

    @Override
    public void createXATransactionFromLocalTransaction(int format_id,byte[] global_id,byte[] branch_id) throws StandardException{
    }

    @Override
    public int xa_prepare() throws StandardException{
        return 0;
    }

    @Override
    public boolean isIdle(){
        return (state==IDLE);
    }

    @Override
    public boolean isPristine(){
        return (state==IDLE || state==ACTIVE);
    }

    @Override
    public void xa_rollback() throws StandardException{
        abort();
    }

    @Override
    public ContextManager getContextManager(){
        return transContext.getContextManager();
    }

    @Override
    public CompatibilitySpace getCompatibilitySpace(){
        return compatibilitySpace;
    }

    @Override
    public void setNoLockWait(boolean noWait){
    }

    @Override
    public void setup(PersistentSet set) throws StandardException{
    }

    @Override
    public GlobalTransactionId getGlobalId(){
        return null;
    }

    @Override
    public void xa_commit(boolean onePhase) throws StandardException{
        SpliceLogUtils.debug(LOG,"xa_commit");
        try{
            if(onePhase)
                commit();
            else{
                xa_prepare();
                commit();
            }
        }catch(Exception e){
            throw StandardException.newException(e.getMessage(),e);
        }
    }

    public abstract TxnView getTxnInformation();

    public abstract void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table);

    public abstract void setActiveState(boolean nested,boolean additive,TxnView parentTxn);

    public TxnView getActiveStateTxn(){
        setActiveState(false,false,null);
        return getTxnInformation();
    }
}
