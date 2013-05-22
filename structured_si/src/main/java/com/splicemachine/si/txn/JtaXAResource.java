package com.splicemachine.si.txn;

import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;

/**
 * View hbase as a JTA transactional resource. This allows it to participate in transactions across multiple resources.
 */
public class JtaXAResource implements XAResource {
    static final Log LOG = LogFactory.getLog(JtaXAResource.class);
    private Map<Xid, TransactionId> xidToTransactionState = new HashMap<Xid, TransactionId>();
    private final Transactor transactor;
    private ThreadLocal<TransactionId> threadLocalTransactionState = new ThreadLocal<TransactionId>();
    private int transactionTimeout = 60;

    public JtaXAResource(final Transactor transactor) {
        this.transactor = transactor;
    }

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        if (LOG.isInfoEnabled())
            LOG.info("commit [" + xid.toString() + "] " + (onePhase ? "one phase" : "two phase"));
        TransactionId state = xidToTransactionState.remove(xid);
        if (state == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            if (onePhase) {
                transactor.commit(state);
            } else {
                transactor.commit(state);
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
        TransactionId state = xidToTransactionState.remove(xid);
        if (state != null) {
            try {
                transactor.rollback(state);
            } catch (Exception e) {
                XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                throw xae;
            }
        }
    }

    public int getTransactionTimeout() throws XAException {
        return transactionTimeout;
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
        return 0;
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
        transactionTimeout = seconds;
        return true;
    }
    @Override
    public void start(final Xid xid, final int flags) throws XAException {
        LOG.info("start [" + xid.toString() + "] ");
        try {
            TransactionId state = transactor.beginTransaction();
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
    public TransactionId getThreadLocalTransactionState() {
        return threadLocalTransactionState.get();
    }

}