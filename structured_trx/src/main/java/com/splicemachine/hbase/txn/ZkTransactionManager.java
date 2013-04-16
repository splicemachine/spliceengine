package com.splicemachine.hbase.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.splicemachine.constants.TransactionConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.splicemachine.hbase.txn.coprocessor.region.TxnUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class ZkTransactionManager extends TransactionManager {
    static final Logger LOG = Logger.getLogger(ZkTransactionManager.class);
    protected String transactionPath;
    protected JtaXAResource xAResource;
  
    public ZkTransactionManager(final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = conf.get(TransactionConstants.TRANSACTION_PATH_NAME, TransactionConstants.DEFAULT_TRANSACTION_PATH);
    }

    public ZkTransactionManager(final String transactionPath, final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = transactionPath;
    }
    
    public ZkTransactionManager(final Configuration conf, ZooKeeperWatcher zkw, RecoverableZooKeeper rzk) throws IOException {
    	this.transactionPath = conf.get(TransactionConstants.TRANSACTION_PATH_NAME, TransactionConstants.DEFAULT_TRANSACTION_PATH);
    	this.zkw = zkw;
    	this.rzk = rzk;
    }

    public ZkTransactionManager(final String transactionPath, ZooKeeperWatcher zkw, RecoverableZooKeeper rzk) throws IOException {
    	this.transactionPath = transactionPath;
    	this.zkw = zkw;
    	this.rzk = rzk;
    }

    public TransactionState beginTransaction(boolean allowWrites, boolean nested, boolean dependent, String parentTransactionID)
            throws KeeperException, InterruptedException, IOException, ExecutionException {
    	SpliceLogUtils.trace(LOG, "Begin transaction");
    	return new TransactionState(TxnUtils.beginTransaction(transactionPath, zkw));
    }
   
    public int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
    	SpliceLogUtils.trace(LOG, "Do prepareCommit on " + transactionState.getTransactionID());
    	TxnUtils.prepareCommit(transactionState.getTransactionID(), rzk);
    	return 0;
     }

    @Override
    public void prepareCommit2(Object bonus, TransactionState transactionState) throws KeeperException, InterruptedException, IOException {
        RecoverableZooKeeper recoverableZooKeeper = (RecoverableZooKeeper) bonus;
        recoverableZooKeeper.setData(transactionState.getTransactionID(), Bytes.toBytes(TransactionStatus.PREPARE_COMMIT.toString()), -1);
    }

    public void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException  {
    	SpliceLogUtils.trace(LOG, "Do commit on " + transactionState.getTransactionID());
    	TxnUtils.doCommit(transactionState.getTransactionID(), rzk);
    }

    public void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.trace(LOG, "Try commit on " +transactionState.getTransactionID());
       	prepareCommit(transactionState);
       	doCommit(transactionState);
    }
    
    public void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException {
    	SpliceLogUtils.trace(LOG, "Abort on " +transactionState.getTransactionID());
    	TxnUtils.abort(transactionState.getTransactionID(), rzk);
     }

    public synchronized JtaXAResource getXAResource() {
        if (xAResource == null) {
            xAResource = new JtaXAResource(this);
        }
        return xAResource;
    }
}
