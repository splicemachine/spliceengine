package com.splicemachine.si.txn;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;

/**
 * View hbase as a JTA transactional resource. This allows it to participate in transactions across multiple resources.
 */
public class JtaXAResource implements XAResource {
    private static final Logger LOG = Logger.getLogger(JtaXAResource.class);
    private Map<Xid, TransactionId> xidToTransactionState = new HashMap<Xid, TransactionId>();
    private final Transactor transactor;
		private final TransactionManager control;
    private ThreadLocal<TransactionId> threadLocalTransactionState = new ThreadLocal<TransactionId>();
    private int transactionTimeout = 60;

    public JtaXAResource(final Transactor transactor,TransactionManager control) {
        this.transactor = transactor;
				this.control = control;
    }

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
    	SpliceLogUtils.trace(LOG, "commit [%s] with onePhase %s",xid, onePhase);
        TransactionId state = xidToTransactionState.remove(xid);
        if (state == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            if (onePhase) {
                control.commit(state);
            } else {
                control.commit(state);
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
    	SpliceLogUtils.trace(LOG, "end [%s]",xid);
        threadLocalTransactionState.remove();
    }
    @Override
    public void forget(final Xid xid) throws XAException {
    	SpliceLogUtils.trace(LOG, "forget [%s]",xid);
        threadLocalTransactionState.remove();
        TransactionId state = xidToTransactionState.remove(xid);
        if (state != null) {
            try {
                control.rollback(state);
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
    	SpliceLogUtils.trace(LOG,"prepare[%s]",xid);
        return 0;
    }

    @Override
    public Xid[] recover(final int flag) throws XAException {
        return xidToTransactionState.keySet().toArray(new Xid[] { });
    }
    @Override
    public void rollback(final Xid xid) throws XAException {
    	SpliceLogUtils.trace(LOG,"rollback [%s]",xid);
        forget(xid);
        threadLocalTransactionState.remove();
    }

    public boolean setTransactionTimeout(final int seconds) throws XAException {
        transactionTimeout = seconds;
        return true;
    }
    @Override
    public void start(final Xid xid, final int flags) throws XAException {
    	SpliceLogUtils.trace(LOG, "start [%s]",xid);
        try {
            TransactionId state = control.beginTransaction();
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