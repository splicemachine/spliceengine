package com.ir.hbase.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.ir.constants.TxnConstants;
import com.ir.hbase.txn.coprocessor.region.TxnUtils;

public class ZkTransactionManager extends TransactionManager {
    static final Log LOG = LogFactory.getLog(ZkTransactionManager.class);
    protected String transactionPath;
    protected JtaXAResource xAResource;
  
    public ZkTransactionManager(final Configuration conf) throws IOException {
    	this(conf.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH),conf);
    }

    public ZkTransactionManager(final String transactionPath, final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = transactionPath;
    }
    public TransactionState beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Begin transaction.");
    	return new TransactionState(TxnUtils.beginTransaction(transactionPath, zkw));
    }
   
    public int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Do prepareCommit on " + transactionState.getTransactionID());
    	TxnUtils.prepareCommit(transactionState.getTransactionID(), rzk);
    	return 0;
     }

    public void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException  {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Do commit on " + transactionState.getTransactionID());
    	TxnUtils.doCommit(transactionState.getTransactionID(), rzk);
    }

    public void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Try commit on " +transactionState.getTransactionID());
       	prepareCommit(transactionState);
       	doCommit(transactionState);
    }
    
    public void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	if (LOG.isDebugEnabled()) 
    		LOG.debug("Abort on " +transactionState.getTransactionID());
    	TxnUtils.abort(transactionState.getTransactionID(), rzk);
     }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
}
