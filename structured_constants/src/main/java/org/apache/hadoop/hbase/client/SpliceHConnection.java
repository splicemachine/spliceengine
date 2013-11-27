package org.apache.hadoop.hbase.client;

import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/20/13
 */
public class SpliceHConnection extends HConnectionManager.HConnectionImplementation{
		private volatile ZooKeeperWatcher zooKeeper;
		private volatile boolean closed = false;

		public static HConnection createConnection(Configuration conf) throws ZooKeeperConnectionException {
				HConnectionManager.HConnectionKey connectionKey = new HConnectionManager.HConnectionKey(conf);
				synchronized (HConnectionManager.HBASE_INSTANCES) {
						HConnection connection = HConnectionManager.HBASE_INSTANCES.get(connectionKey);
						if (connection == null) {
								connection = new SpliceHConnection(conf, true);
								HConnectionManager.HBASE_INSTANCES.put(connectionKey, (HConnectionManager.HConnectionImplementation) connection);
						}
//								} else if (connection.isClosed()) {
//										HConnectionManager.deleteConnection(connectionKey, true);
//										connection = new HConnectionImplementation(conf, true);
//										HBASE_INSTANCES.put(connectionKey, connection);
//								}
//								connection.incCount();
						return connection;
				}
		}
		/**
		 * constructor
		 *
		 * @param conf Configuration object
		 */
		public SpliceHConnection(Configuration conf, boolean managed) throws ZooKeeperConnectionException {
				super(conf, managed);
		}

		@Override
		public synchronized ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException {
				if(zooKeeper==null){
						if(this.isClosed()){
								try {
										throw new IOException(toString()+" closed");
								} catch (IOException e) {
										throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper",e);
								}
						}
						zooKeeper = ZkUtils.getZooKeeperWatcher();
				}
				return zooKeeper;
		}

		@Override
		public boolean isClosed() {
				return false;
		}

		@Override
		public void close() {
				//no-op
		}
}
