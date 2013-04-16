package com.splicemachine.derby.utils;

import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Utilities related to ZooKeeper.
 *
 * @author Scott Fines
 * Created: 2/2/13 9:38 AM
 */
public class ZkUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);
	private static final SpliceZooKeeperManager zkManager = new SpliceZooKeeperManager();

	/**
	 * Gets a direct interface to a ZooKeeper instance.
	 *
	 * @return a direct interface to ZooKeeper.
	 */
	public static RecoverableZooKeeper getRecoverableZooKeeper(){
		try {
			return zkManager.getRecoverableZooKeeper();
		} catch (ZooKeeperConnectionException e) {
			LOGGER.error("Unable to connect to zookeeper, aborting",e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return direct interface to a ZooKeeperWatcher
	 */
	public static ZooKeeperWatcher getZooKeeperWatcher(){
		try {
			return zkManager.getZooKeeperWatcher();
		} catch (ZooKeeperConnectionException e) {
			LOGGER.error("Unable to connect to zookeeper, aborting",e);
			throw new RuntimeException(e);
		}
	}


	/**
	 * Creates a node in ZooKeeper if it does not already exist. If it already exists, this does nothing.
	 *
	 * @param path the path to create
	 * @param bytes the bytes to set
	 * @param acls the privileges for the new node
	 * @param createMode the create mode for that node
	 * @throws IOException if something goes wrong and the node can't be added.
	 */
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


    public static boolean recursiveSafeCreate(String path,byte[] bytes, List<ACL> acls, CreateMode createMode) throws InterruptedException, KeeperException {
        if(path==null||path.length()<=0) return true; //nothing to do, we've gone all the way to the root
        RecoverableZooKeeper rzk = getRecoverableZooKeeper();
        try{
            return safeCreate(path,bytes,acls,createMode,rzk);
        }catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE){
                //parent node doesn't exist, so recursively create it, and then try and create your node again
                String parent = path.substring(0, path.lastIndexOf('/'));
                recursiveSafeCreate(parent,new byte[]{},acls,CreateMode.PERSISTENT);
                return safeCreate(path,bytes,acls,createMode);
            }
            else
                throw e;
        }
    }

    private static boolean safeCreate(String path, byte[] bytes, List<ACL> acls, CreateMode createMode,RecoverableZooKeeper zooKeeper) throws KeeperException,InterruptedException{
       try{
           zooKeeper.create(path,bytes,acls,createMode);
           return true;
       }catch(KeeperException ke){
           if(ke.code()!= KeeperException.Code.NODEEXISTS)
               throw ke;
           else
               return true;
       }
    }
	/**
	 * Sets the data onto ZooKeeper.
	 *
	 * This is essentially a wrapper method to clean up exception handling code.
	 *
	 * @param path the path to set data on
	 * @param data the data to set
	 * @param version the version to set (-1 if you don't care).
	 * @throws IOException if something goes wrong and the data can't be set.
	 */
	public static void setData(String path, byte[] data, int version) throws IOException{
		try {
			getRecoverableZooKeeper().setData(path,data,version);
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Gets data from ZooKeeper.
	 *
	 * This is essentially a wrapper method to clean up exception handling code.
	 *
	 * @param path the path to get data from
	 * @return the data contained in that path
	 * @throws IOException if something goes wrong and the data can't be set.
	 */
	public static byte[] getData(String path) throws IOException{
		try{
			return getRecoverableZooKeeper().getData(path,false,null);
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (KeeperException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Increments a counter stored in ZooKeeper
	 *
	 * @param counterNode the node to store the counter in
	 * @return the next counter number
	 * @throws IOException if something goes wrong and the counter can't be incremented.
	 */
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
