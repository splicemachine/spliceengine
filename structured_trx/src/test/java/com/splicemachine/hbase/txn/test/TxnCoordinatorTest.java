package com.splicemachine.hbase.txn.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.junit.Before;
import org.junit.Test;
import com.splicemachine.hbase.txn.TransactionStatus;
import com.splicemachine.hbase.txn.TxnCoordinator;
import com.splicemachine.hbase.txn.TxnCoordinator2;

public class TxnCoordinatorTest extends ZkBaseTest {
	private static final Log LOG = LogFactory.getLog(TxnCoordinatorTest.class);

	public static final String TRANSACTION_ID = "/TRANSACTION_ID";
	public static final int watcherNum = 30;
	public static List<Worker> workers = new ArrayList<Worker>();
	public static String path;
	public static Random generator = new Random();
	
	public static int getRandom() {
		return (generator.nextInt(100) + 1);
	}

	public static String getPath(String path, String child) {
		return path + "/" + child;
	}

	public static class Worker {
		public Worker(int i) throws KeeperException, InterruptedException {
			final String subPath = rzk.create(getPath(path, Integer.toString(i)), Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			rzk.getData(path, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (LOG.isDebugEnabled())
						LOG.debug("Subpath " + subPath + " process event " + event.getType().toString());
					if (event.getType().equals(Event.EventType.NodeDataChanged)) {
						try {
							TransactionStatus status = TransactionStatus.valueOf(Bytes.toString(rzk.getData(path, this, null)));
							switch(status) {
							case PREPARE_COMMIT:
								if (LOG.isDebugEnabled())
									LOG.debug("Subpath " + subPath + " prepare commit.");
								Thread.sleep(getRandom());
								rzk.setData(subPath, TransactionStatus.PREPARE_COMMIT.toString().getBytes(), -1);
								break;
							case DO_COMMIT:
								if (LOG.isDebugEnabled())
									LOG.debug("Subpath " + subPath + " do commit.");
								Thread.sleep(50);
								rzk.setData(subPath, TransactionStatus.DO_COMMIT.toString().getBytes(), -1);
								rzk.delete(subPath, -1);
							}
						} catch (KeeperException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
					}
				}
			}, null);
		}
	}

	@Before
	public void setUpZk() throws Exception {
		if (path != null)
			ZKUtil.deleteRecursive(rzk.getZooKeeper(), path);
		path = rzk.create(TRANSACTION_ID, Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		for (int i = 0; i < watcherNum; ++i) {
			workers.add(new Worker(i));
		}
		if (LOG.isDebugEnabled())
			LOG.debug("Children size " + rzk.getChildren(path, false).size());
	}

	@Test
	public void testPrepareCommit() throws Exception {
		long startTime = System.currentTimeMillis();
		TxnCoordinator coordinator = new TxnCoordinator(rzk);
		rzk.setData(path, TransactionStatus.PREPARE_COMMIT.toString().getBytes(), -1);
		if (LOG.isDebugEnabled())
			LOG.debug("Root path " + path);
		boolean abort = coordinator.watch(path, TransactionStatus.PREPARE_COMMIT);
		if (LOG.isDebugEnabled())
			LOG.debug("Prepare commit Abort? " + abort + ", using time: " + (System.currentTimeMillis() - startTime));
	}
	
	@Test
	public void testManySmallCommit() throws Exception {
		long startTime = System.currentTimeMillis();
		TxnCoordinator coordinator = new TxnCoordinator(rzk);
		rzk.setData(path, TransactionStatus.DO_COMMIT.toString().getBytes(), -1);
		if (LOG.isDebugEnabled())
			LOG.debug("Root path " + path);
		boolean abort = coordinator.watch(path, TransactionStatus.DO_COMMIT);
		if (LOG.isDebugEnabled())
			LOG.debug("Do commit Abort? " + abort + ", using time: " + (System.currentTimeMillis() - startTime));
		Assert.assertEquals(0, rzk.getChildren(path, false).size());
	}
	
	@Test
	public void testManaySmallPrepareCommit() throws Exception {
		for (int i = 0; i < 101; ++i) {
			path = rzk.create(TRANSACTION_ID, Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			Worker worker = new Worker(1);
			long startTime = System.currentTimeMillis();
			TxnCoordinator2 coordinator = new TxnCoordinator2(rzk);
			rzk.setData(path, TransactionStatus.PREPARE_COMMIT.toString().getBytes(), -1);
			if (LOG.isDebugEnabled())
				LOG.debug("Root path " + path);
			boolean abort = coordinator.watch(path, TransactionStatus.PREPARE_COMMIT);
			if (LOG.isDebugEnabled())
				LOG.debug("Prepare commit Abort? " + abort + ", using time: " + (System.currentTimeMillis() - startTime));
		}
	}
	
	@Test
	public void testCommit() throws Exception {
		for (int i = 0; i < 101; ++i) {
			path = rzk.create(TRANSACTION_ID, Bytes.toBytes(TransactionStatus.PENDING.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			Worker worker = new Worker(1);
			long startTime = System.currentTimeMillis();
			TxnCoordinator2 coordinator = new TxnCoordinator2(rzk);
			rzk.setData(path, TransactionStatus.DO_COMMIT.toString().getBytes(), -1);
			if (LOG.isDebugEnabled())
				LOG.debug("Root path " + path);
			boolean abort = coordinator.watch(path, TransactionStatus.DO_COMMIT);
			if (LOG.isDebugEnabled())
				LOG.debug("Prepare commit Abort? " + abort + ", using time: " + (System.currentTimeMillis() - startTime));
		}
	}
	




}
