package com.splicemachine.hbase.txn;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import com.splicemachine.constants.TransactionStatus;
import com.splicemachine.constants.TxnConstants;

public class TxnCoordinator2 extends TxnConstants {
	private static final Log LOG = LogFactory.getLog(TxnCoordinator.class);
	private RecoverableZooKeeper zk; 
	private long timeout;
	private CountDownLatch latch;
	private volatile boolean abort = false;
	private TransactionStatus objective;

	public TxnCoordinator2(RecoverableZooKeeper zk) {
		this(zk, LATCH_TIMEOUT);
	}

	public TxnCoordinator2(RecoverableZooKeeper zk, long timeout) {
		this.zk = zk;
		this.timeout = timeout;
	}

	public boolean watch(String path, TransactionStatus objective) throws KeeperException, InterruptedException {
		this.objective = objective;
		List<String> children = zk.getChildren(path, false);
		if (LOG.isDebugEnabled())
			LOG.debug("Latch size: " + children.size());
		if (children.size() > 0) {
			latch = new CountDownLatch(children.size());
			for (String childPath: children) {
				if (LOG.isDebugEnabled())
					LOG.debug("Evaluating Child Path in Executor Service : " + childPath);
				new CohortWatcher(path+"/"+childPath);
			}
			latch.await(timeout, TimeUnit.SECONDS);
			if (LOG.isDebugEnabled())
				LOG.debug("Done watch.");
		}
		return abort;
	}
	
	

	private class CohortWatcher implements Watcher {
		private String subPath;
		private boolean cond = true;
		
		private synchronized void countDown() {
			if (cond) {
				cond = false;
				try {
					zk.getData(subPath, false, null);
				} catch (KeeperException e) {
					LOG.error("CohortWatcher retire watcher error ", e);
				} catch (InterruptedException e) {
					LOG.error("CohortWatcher retire watcher error ", e);
				}
				latch.countDown();
			}
		}

		public CohortWatcher(String subPath) {
			this.subPath = subPath;
			try {
				TransactionStatus status = TransactionStatus.valueOf(Bytes.toString(zk.getData(subPath, this, null)));
				if (status.equals(TransactionStatus.ABORT)) {
					abort = true;
					countDown();
				} else if (status.equals(objective)) {
					countDown();
				}
			} catch (KeeperException e) {
				countDown();
				LOG.error("CohortWatcher setUp error ", e);
			} catch (InterruptedException e) {
				countDown();
				LOG.error("CohortWatcher setUp error ", e);
			}
		}
		@Override
		public void process(WatchedEvent event) {
			try {
				if (event.getType().equals(Event.EventType.NodeDataChanged)) {
					TransactionStatus status = TransactionStatus.valueOf(Bytes.toString(zk.getData(subPath, this, null)));
					if (status.equals(objective)) {
						countDown();
					} else if (status.equals(TransactionStatus.ABORT)) {
						abort = true;
						countDown();
					}
				}
			} catch (KeeperException e) {
				countDown();
				LOG.error("CohortWatcher process error ", e);
			} catch (InterruptedException e) {
				countDown();
				LOG.error("CohortWatcher process error ", e);
			}
		}
	}
}
