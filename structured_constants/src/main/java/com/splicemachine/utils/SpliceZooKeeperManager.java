package com.splicemachine.utils;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.splicemachine.constants.SpliceConstants;
import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created: 2/2/13 9:47 AM
 */
public class SpliceZooKeeperManager extends SpliceConstants implements Abortable,Closeable {
	private static final Logger LOG = Logger.getLogger(SpliceZooKeeperManager.class);
	private ZooKeeperWatcher watcher;
	private RecoverableZooKeeper rzk;
	private volatile boolean isAborted;

	public ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException {
		synchronized (this){
			if(watcher==null) {
				try {
					watcher = new ZooKeeperWatcher(config,"spliceconnection",this);
				} catch (IOException e) {
					throw new ZooKeeperConnectionException("Unable to connect to zookeeper",e);
				}
			}
			return watcher;
		}
	}

	public RecoverableZooKeeper getRecoverableZooKeeper() throws ZooKeeperConnectionException {
		synchronized (this){
			if(rzk==null) {
				ZooKeeperWatcher zkw = getZooKeeperWatcher();
				rzk = zkw.getRecoverableZooKeeper();
			}
			return rzk;
		}
	}

	@Override
	public void abort(String why, Throwable e) {
		if (e instanceof KeeperException.SessionExpiredException){
			try{
				LOG.info("Lost connection with ZooKeeper, attempting reconnect");
				watcher = null;
				getZooKeeperWatcher();
				LOG.info("Successfully reconnected to ZooKeeper");
			}catch(ZooKeeperConnectionException zce){
				LOG.error("Could not reconnect to zookeeper after session expiration, aborting");
				e = zce;
			}
		}
		if(e!=null)
			LOG.error(why,e);
		else LOG.error(why);
		this.isAborted=true;
	}

	@Override
	public boolean isAborted() {
		return isAborted;
	}

	@Override
	public void close() throws IOException {
		if(watcher!=null)watcher.close();
		if(rzk!=null) try {
			rzk.close();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

}
