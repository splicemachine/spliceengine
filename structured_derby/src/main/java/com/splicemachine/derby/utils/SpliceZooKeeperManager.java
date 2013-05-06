package com.splicemachine.derby.utils;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created: 2/2/13 9:47 AM
 */
public class SpliceZooKeeperManager implements Abortable,Closeable {
	private static final Logger LOG = Logger.getLogger(SpliceZooKeeperManager.class);
	private ZooKeeperWatcher watcher;
	private RecoverableZooKeeper rzk;
	private volatile boolean isAborted;

	public ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException {
		synchronized (this){
			if(watcher==null) {
				try {
					watcher = new ZooKeeperWatcher(SpliceUtils.config,"spliceconnection",this);
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

    public interface Command<T>{
        T execute(RecoverableZooKeeper zooKeeper) throws InterruptedException,KeeperException;
    }

    public <T> T executeUnlessExpired(Command<T> command) throws InterruptedException,KeeperException{
        /*
         * What actually happens is that, in the event of a long network partition, ZooKeeper will throw
         * ConnectionLoss exceptions, but it will NOT throw a SessionExpired exception until it reconnects, even
         * if it's been disconnected for CLEARLY longer than the session timeout.
         *
         * To deal with this, we have to basically loop through our command repeatedly until we either
         *
         * 1. Succeed.
         * 2. Get a SessionExpired event from ZooKeeper
         * 3. Spent more than 2*sessionTimeout ms attempting the request
         * 4. Get some other kind of Zk error (NoNode, etc).
         */
        RecoverableZooKeeper rzk;
        try {
            rzk = getRecoverableZooKeeper();
        } catch (ZooKeeperConnectionException e) {
            throw new KeeperException.SessionExpiredException();
        }
        //multiple by 2 to make absolutely certain we're timed out.
        int sessionTimeout = 2*rzk.getZooKeeper().getSessionTimeout();
        long nextTime = System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        while((int)(nextTime-startTime)<sessionTimeout){
            try{
                return command.execute(rzk);
            }catch(KeeperException ke){
                switch (ke.code()) {
                    case CONNECTIONLOSS:
                    case OPERATIONTIMEOUT:
                        SpliceLogUtils.warn(LOG,"Detected a Connection issue(%s) with ZooKeeper, retrying",ke.code());
                        nextTime = System.currentTimeMillis();
                        break;
                    default:
                        throw ke;
                }
            }
        }

        //we've run out of time, our session has almost certainly expired. Give up and explode
        throw new KeeperException.SessionExpiredException();
    }

    public <T> T execute(Command<T> command) throws InterruptedException,KeeperException{
        try {
            return command.execute(getRecoverableZooKeeper());
        } catch (ZooKeeperConnectionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }
}
