package com.splicemachine.si2.txn;

import com.splicemachine.si2.si.api.TransactionId;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class TransactionManager {
    static final Logger LOG = Logger.getLogger(TransactionManager.class);
    protected JtaXAResource xAResource;
    private final Transactor transactor;

    public TransactionManager(final Transactor transactor) throws IOException {
        this.transactor = transactor;
    }

    public TransactionId beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException {
        SpliceLogUtils.trace(LOG, "Begin transaction");
        return transactor.beginTransaction();
    }

    public int prepareCommit(final TransactionId transaction) throws KeeperException, InterruptedException, IOException {
        SpliceLogUtils.trace(LOG, "prepareCommit %s", transaction);
        return 0;
    }

    public void doCommit(final TransactionId transaction) throws KeeperException, InterruptedException, IOException {
        SpliceLogUtils.trace(LOG, "doCommit %s", transaction);
        transactor.commit(transaction);
    }

    public void tryCommit(final TransactionId transaction) throws IOException, KeeperException, InterruptedException {
        SpliceLogUtils.trace(LOG, "tryCommit %s", transaction);
        prepareCommit(transaction);
        doCommit(transaction);
    }

    public void abort(final TransactionId transaction) throws IOException, KeeperException, InterruptedException {
        SpliceLogUtils.trace(LOG, "abort %s", transaction);
        transactor.abort(transaction);
    }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
}