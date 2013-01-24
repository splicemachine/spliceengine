package com.splicemachine.hbase.txn;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

public class TxnCoordinator {
	private static final Log LOG = LogFactory.getLog(TxnCoordinator.class);
	private RecoverableZooKeeper zk; 
	private long timeout;
	private ExecutorService pool = Executors.newCachedThreadPool();
	private volatile boolean abort = false;

	public TxnCoordinator(RecoverableZooKeeper zk) {
		this(zk, TxnConstants.LATCH_TIMEOUT);
	}

	public TxnCoordinator(RecoverableZooKeeper zk, long timeout) {
		this.zk = zk;
		this.timeout = timeout;
	}

	public boolean watch(String path, TransactionStatus objective) throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(path, false);
		if (children.size() > 0) {
			if (LOG.isInfoEnabled())
				LOG.info("Latch size: " + children.size());
			for (String childPath: children) {
				if (LOG.isInfoEnabled())
					LOG.info("Evaluating Child Path in Executor Service : " + childPath);
				pool.execute(new CohortWatcher(path+"/"+childPath, objective));
			}
			pool.shutdown();
			pool.awaitTermination(timeout, TimeUnit.SECONDS);
			if (LOG.isInfoEnabled())
				LOG.info("Done watch.");
		}
		return abort;
	}

	private class CohortWatcher implements Runnable, Watcher {
		private String path;
		private TransactionStatus status;
		private TransactionStatus objective;
		private boolean cond;

		public CohortWatcher(String path, TransactionStatus objective) {
			this.path = path;
			this.objective = objective;
			this.cond = true;
		}

		@Override
		public void process(WatchedEvent event) {
			try {
				if (event.getType().equals(Event.EventType.NodeDeleted)) {
					cond = false;
				} else if (event.getType().equals(Event.EventType.NodeDataChanged)) {
					if (TransactionStatus.valueOf(Bytes.toString(zk.getData(path, this, null))).equals(objective)) {
						cond = false;
					} else if (TransactionStatus.valueOf(Bytes.toString(zk.getData(path, this, null))).equals(TransactionStatus.ABORT)) {
						cond = false;
						abort = true;
					}
				}
			} catch(KeeperException.NoNodeException ne) {
				cond = false;
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		@Override
		public void run() {
			try {
				if (zk.exists(path, this) != null) {
					status = TransactionStatus.valueOf(Bytes.toString(zk.getData(path, this, null)));
					if (status.equals(TransactionStatus.ABORT)) {
						abort = true;
					} else if (!status.equals(objective)) {
						while (cond) {
							Thread.sleep(100);
						}
					}
				}
			} catch(KeeperException.NoNodeException ne) {
				cond = false;
			} catch (KeeperException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}
}
