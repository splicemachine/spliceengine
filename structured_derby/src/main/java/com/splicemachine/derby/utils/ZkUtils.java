package com.splicemachine.derby.utils;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 * Created: 2/2/13 9:38 AM
 */
public class ZkUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);
	private static final SpliceZooKeeperManager zkManager = new SpliceZooKeeperManager();

	public static RecoverableZooKeeper getRecoverableZooKeeper(){
		try {
			return zkManager.getRecoverableZooKeeper();
		} catch (ZooKeeperConnectionException e) {
			LOGGER.error("Unable to connect to zookeeper, aborting",e);
			throw new RuntimeException(e);
		}
	}

	public static ZooKeeperWatcher getZooKeeperWatcher(){
		try {
			return zkManager.getZooKeeperWatcher();
		} catch (ZooKeeperConnectionException e) {
			LOGGER.error("Unable to connect to zookeeper, aborting",e);
			throw new RuntimeException(e);
		}
	}

	public static void addIfAbsent(String path, byte[] bytes,
																 List<ACL> acls, CreateMode createMode) throws IOException {
		try {
			if(getRecoverableZooKeeper().exists(path,false)==null){
				safeCreate(path,bytes,acls, createMode);
			}
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public static boolean safeCreate(String path, byte[] bytes, List<ACL> acls, CreateMode createMode)
																														throws KeeperException, InterruptedException {
		try {
			getRecoverableZooKeeper().create(path,bytes,acls,createMode);
			return true;
		} catch (KeeperException e) {
			if(e.code()== KeeperException.Code.NODEEXISTS){
				//it's already been created, so nothing to do
				return true;
			}
			throw e;
		}
	}

	public static void setData(String path, byte[] data, int version) throws IOException{
		try {
			ZkUtils.getRecoverableZooKeeper().setData(path,data,version);
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public static byte[] getData(String path) throws IOException{
		try{
			return getRecoverableZooKeeper().getData(path,false,null);
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (KeeperException e) {
			throw new IOException(e);
		}
	}

	public static long nextSequenceId(String counterNode) throws IOException {
		/*
		 * When generating a new, Monotonically increasing identifier
		 * based off of zookeeper, you have essentially two choices:
		 *
		 * 1. Create a new sequential znode under a base znode, then
		 * read off the assigned sequential identifier.
		 * 2. Use optimistic versioning to push increments to a single znode
		 * atomically.
		 *
		 * Case 1 is the simplest to implement, but it has some notable issues:
		 *  1. There is no contiguity of numbers. ZooKeeper guarantees
		 *  monotonically increasing sequence numbers(Up to Integer.MAX_VALUE
		 *  at any rate), but does not guarantee contiguiity of those numbers.
		 *  Thus, it's possible to see 1, then the next be 10
		 * 2. Lots of znodes. If you have a few thousand conglomerates, then
		 * you'll have a few thousand znodes, whose only purpose was to
		 * grab the sequential identifier. Not only does this add to the
		 * conceptual difficulty in understanding the zookeeper layout, but it
		 * also adds additional load to the ZooKeeper cluster itself.
		 * 3. Rollovers. The sequential identifier supplied by zookeeper is
		 * limited to integers, which can cause a rollover eventually.
		 * 4. Expensive to read. To get the highest sequence element (without
		 * updating), you must get *all* the znode children, then sort them
		 * by sequence number, and then pick out the highest (or lowest). This
		 * is a large network operation when there are a few thousand znodes.
		 * 5. It's difficult to watch. You can watch on children, but in
		 * order to act on them, you must first
		 *
		 * this implementation uses the second option
		 */

		RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
		int maxTries=10;
		int tries=0;
		while(tries<=maxTries){
			tries++;
			//get the current state of the counter
			Stat version = new Stat();
			long currentCount;
			byte[] data;
			try {
				data = rzk.getData(counterNode,false,version);
			} catch (KeeperException e) {
				//if we timed out trying to deal with ZooKeeper, retry
				switch (e.code()) {
					case CONNECTIONLOSS:
					case OPERATIONTIMEOUT:
						continue;
					default:
						throw new IOException(e);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			currentCount = Bytes.toLong(data)+1;

			//try and write the next entry, but only if the versions haven't changed
			try {
				rzk.setData(counterNode, Bytes.toBytes(currentCount),version.getVersion());
				return currentCount;
			} catch (KeeperException e) {
				//if we timed out talking, or if the version didn't match, retry
				switch (e.code()) {
					case CONNECTIONLOSS:
					case OPERATIONTIMEOUT:
					case BADVERSION:
						continue;
					default:
						throw new IOException(e);
				}
			} catch (InterruptedException e) {
				//we were interrupted, which means we need to bail
				throw new IOException(e);
			}
		}
		/*
		 * We've tried to get the latest sequence number a whole bunch of times,
		 * without success. That means either a problem with ZooKeeper, or a LOT
		 * of contention in getting the sequence. Need to back off and deal with it
		 * at a higher level
		 *
		 * TODO -sf- we could fix this by putting in a non-optimistic lock at this point, but
		 * it's probably best to see if it poses a problem as written first.
		 */
		throw new IOException("Unable to get next conglomerate sequence, there is an issue" +
				"speaking with ZooKeeper");
	}
}
