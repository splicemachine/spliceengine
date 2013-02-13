package com.splicemachine.iapi.txn;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import com.splicemachine.impl.si.txn.JtaXAResource;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.utils.SIConstants;


public abstract class TransactionManager extends SIConstants {
	private static Logger LOG = Logger.getLogger(TransactionManager.class);
    protected RecoverableZooKeeper rzk;

    
    public TransactionManager() {}
    
	public TransactionManager(Configuration config) {
		try {
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
	public RecoverableZooKeeper getRecoverableZooKeeper() {
		return rzk;
	}

	public abstract Transaction beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException;

	public abstract int prepareCommit(final Transaction transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void doCommit(final Transaction transactionState) throws KeeperException, InterruptedException, IOException;

	public abstract void tryCommit(final Transaction transactionState) throws IOException, KeeperException, InterruptedException;

	public abstract void abort(final Transaction transactionState) throws IOException, KeeperException, InterruptedException;
	
	public abstract JtaXAResource getXAResource();

	
	
}
