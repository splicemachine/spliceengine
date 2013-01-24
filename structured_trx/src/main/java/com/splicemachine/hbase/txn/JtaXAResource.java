package com.splicemachine.hbase.txn;

import java.util.HashMap;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * View hbase as a JTA transactional resource. This allows it to participate in transactions across multiple resources.
 */
public class JtaXAResource implements XAResource {

    static final Log LOG = LogFactory.getLog(JtaXAResource.class);

    private Map<Xid, TransactionState> xidToTransactionState = new HashMap<Xid, TransactionState>();
    private final TransactionManager transactionManager;
    private ThreadLocal<TransactionState> threadLocalTransactionState = new ThreadLocal<TransactionState>();

    public JtaXAResource(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }
    
    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
    	if (LOG.isInfoEnabled())
    		LOG.info("commit [" + xid.toString() + "] " + (onePhase ? "one phase" : "two phase"));
        TransactionState state = xidToTransactionState.remove(xid);
        if (state == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            if (onePhase) {
                transactionManager.tryCommit(state);
            } else {
                transactionManager.doCommit(state);
            }
        } catch (Exception e) {
            XAException xae = new XAException(XAException.XAER_RMERR);
            xae.initCause(e);
            throw xae;
        } finally {
            threadLocalTransactionState.remove();
        }

    }
    @Override
    public void end(final Xid xid, final int flags) throws XAException {
        LOG.info("end [" + xid.toString() + "] ");
        threadLocalTransactionState.remove();
    }
    @Override
    public void forget(final Xid xid) throws XAException {
        LOG.info("forget [" + xid.toString() + "] ");
        threadLocalTransactionState.remove();
        TransactionState state = xidToTransactionState.remove(xid);
        if (state != null) {
            try {
                transactionManager.abort(state);
            } catch (Exception e) {
                XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                throw xae;
            }
        }
    }

    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    public boolean isSameRM(final XAResource xares) throws XAException {
        if (xares instanceof JtaXAResource) {
            return true;
        }
        return false;
    }
    @Override
    public int prepare(final Xid xid) throws XAException {
        LOG.info("prepare [" + xid.toString() + "] ");
        TransactionState state = xidToTransactionState.get(xid);
        try {
            return this.transactionManager.prepareCommit(state);
        } catch (Exception e) {
            XAException xae = new XAException(XAException.XA_HEURRB);
            xae.initCause(e);
            throw xae;   
        }
    }
    @Override
    public Xid[] recover(final int flag) throws XAException {
        return xidToTransactionState.keySet().toArray(new Xid[] { });
    }
    @Override
    public void rollback(final Xid xid) throws XAException {
        LOG.info("rollback [" + xid.toString() + "] ");
        forget(xid);
        threadLocalTransactionState.remove();
    }

    public boolean setTransactionTimeout(final int seconds) throws XAException {
        return false; // Currently not supported. (Only global lease time) XXX-TODO Guangle Fan
    }
    @Override
    public void start(final Xid xid, final int flags) throws XAException {
        LOG.info("start [" + xid.toString() + "] ");
        try {
        	TransactionState state = this.transactionManager.beginTransaction();
            threadLocalTransactionState.set(state);
            xidToTransactionState.put(xid, state);

        } catch (Exception e) {
            XAException xae = new XAException(XAException.XA_RBDEADLOCK);
            xae.initCause(e);
            throw xae;  
        }
    }

    /**
     * @return the threadLocalTransaction state.
     */
    public TransactionState getThreadLocalTransactionState() {
        return threadLocalTransactionState.get();
    }

}