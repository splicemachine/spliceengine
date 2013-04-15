package com.splicemachine.impl.si.txn;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.iapi.txn.TransactionManager;
import com.splicemachine.iapi.txn.TransactionState;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class TransactionManagerImpl extends TransactionManager {
    static final Logger LOG = Logger.getLogger(TransactionManagerImpl.class);
    protected String transactionPath;
    protected JtaXAResource xAResource;
    
    static {
    	try {
			@SuppressWarnings("resource")
			HBaseAdmin admin = new HBaseAdmin(new Configuration());
			if (!admin.tableExists(TRANSACTION_TABLE_BYTES)) {
				HTableDescriptor desc = new HTableDescriptor(TRANSACTION_TABLE_BYTES);
				desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
						HBaseConstants.DEFAULT_VERSIONS,
						admin.getConfiguration().get(HBaseConstants.TABLE_COMPRESSION,HBaseConstants.DEFAULT_COMPRESSION),
						HBaseConstants.DEFAULT_IN_MEMORY,
						HBaseConstants.DEFAULT_BLOCKCACHE,
						HBaseConstants.DEFAULT_TTL,
						HBaseConstants.DEFAULT_BLOOMFILTER));
				desc.addFamily(new HColumnDescriptor(TxnConstants.DEFAULT_FAMILY));
				admin.createTable(desc);
			}
			RecoverableZooKeeper recoverableZoo = ZKUtil.connect(admin.getConfiguration(), new Watcher() {			
				@Override
				public void process(WatchedEvent event) {	
				}
			});
			SIUtils.createWithParents(recoverableZoo, admin.getConfiguration().get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH));
    	} catch (Exception e) {
    		SpliceLogUtils.logAndThrowRuntime(LOG, e);
    	}
	}
    
    public TransactionManagerImpl() throws IOException {
    	this(new Configuration());
    }
    
    public TransactionManagerImpl(final Configuration conf) throws IOException {
    	this(conf.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH),conf);
    }

    public TransactionManagerImpl(final String transactionPath, final Configuration conf) throws IOException {
    	super(conf);
    	this.transactionPath = transactionPath;
    }
    
    public Transaction beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException {
    	SpliceLogUtils.trace(LOG, "Begin transaction");
    	Transaction transaction = new Transaction(SIUtils.createIncreasingTimestamp(transactionPath, rzk),TransactionState.ACTIVE);
    	transaction.write();
    	return transaction;
    }
   
    public int prepareCommit(final Transaction transaction) throws KeeperException, InterruptedException, IOException {
    	SpliceLogUtils.trace(LOG, "prepareCommit %s",transaction);
    	transaction.prepareCommit();
    	return 0;
     }

    public void doCommit(final Transaction transaction) throws KeeperException, InterruptedException, IOException  {
    	SpliceLogUtils.trace(LOG, "commit %s",transaction);
    	transaction.doCommit(SIUtils.createIncreasingTimestamp(transactionPath, rzk));
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