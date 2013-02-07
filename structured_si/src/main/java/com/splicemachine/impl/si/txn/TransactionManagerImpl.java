package com.splicemachine.impl.si.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.iapi.txn.TransactionManager;
import com.splicemachine.utils.SpliceLogUtils;

public class TransactionManagerImpl extends TransactionManager {
    static final Logger LOG = Logger.getLogger(TransactionManagerImpl.class);
    protected String transactionPath;
    protected JtaXAResource xAResource;
    public TransactionManagerImpl() throws IOException {
    	super(new Configuration());
    }
    
    public TransactionManagerImpl(final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = conf.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH);
    }

    public TransactionManagerImpl(final String transactionPath, final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = transactionPath;
    }
    
    public Transaction beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException {
    	SpliceLogUtils.trace(LOG, "Begin transaction");
    	return Transaction.beginTransaction();
    }
   
    public int prepareCommit(final Transaction transaction) throws KeeperException, InterruptedException, IOException {
    	SpliceLogUtils.trace(LOG, "prepareCommit %s",transaction);
    	transaction.prepareCommit();
    	return 0;
     }

    public void doCommit(final Transaction transaction) throws KeeperException, InterruptedException, IOException  {
    	SpliceLogUtils.trace(LOG, "doCommit %s",transaction);
    	transaction.doCommit();
    }

    public void tryCommit(final Transaction transaction) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.trace(LOG, "tryCommit %s",transaction);
       	prepareCommit(transaction);
       	doCommit(transaction);
    }
    
    public void abort(final Transaction transaction) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.trace(LOG, "abort %s",transaction);
    	transaction.abort();
    }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
}