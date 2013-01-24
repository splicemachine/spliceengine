package com.splicemachine.hbase.txn;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.TxnConstants;


public abstract class TransactionManager extends TxnConstants {
	
	protected ZooKeeperWatcher zkw;
    protected RecoverableZooKeeper rzk;
	
    public TransactionManager() {}
    
    @SuppressWarnings(value = "deprecation")
	public TransactionManager(Configuration config) {
		try {
			@SuppressWarnings("resource")
			HBaseAdmin admin = new HBaseAdmin(config);
			if (!admin.tableExists(TxnConstants.TRANSACTION_LOG_TABLE_BYTES)) {
				HTableDescriptor desc = new HTableDescriptor(TxnConstants.TRANSACTION_LOG_TABLE_BYTES);
				desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
						HBaseConstants.DEFAULT_VERSIONS,
						HBaseConstants.DEFAULT_COMPRESSION,
//						config.get(HBaseConstants.TABLE_COMPRESSION,HBaseConstants.DEFAULT_COMPRESSION),
						HBaseConstants.DEFAULT_IN_MEMORY,
						HBaseConstants.DEFAULT_BLOCKCACHE,
						HBaseConstants.DEFAULT_TTL,
						HBaseConstants.DEFAULT_BLOOMFILTER));
				desc.addFamily(new HColumnDescriptor(TxnConstants.DEFAULT_FAMILY));
				admin.createTable(desc);
			}
			//zkw = admin.getConnection().getZooKeeperWatcher();
			//rzk = zkw.getRecoverableZooKeeper();
			final CountDownLatch connSignal = new CountDownLatch(1);
			rzk = ZKUtil.connect(config, new Watcher() {			
				@Override
				public void process(WatchedEvent event) {	
					if (event.getState() == KeeperState.SyncConnected)
						connSignal.countDown();
				}
			});
			
			connSignal.await();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public abstract TransactionState beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException;

	public abstract int prepareCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void doCommit(final TransactionState transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void tryCommit(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException;

	public abstract void abort(final TransactionState transactionState) throws IOException, KeeperException, InterruptedException;
	
	public abstract JtaXAResource getXAResource();

}
