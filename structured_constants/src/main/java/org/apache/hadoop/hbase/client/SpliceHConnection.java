package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

/**
 * @author Scott Fines
 *         Date: 11/20/13
 */
public class SpliceHConnection extends HConnectionManager.HConnectionImplementation{
		private volatile ZooKeeperKeepAliveConnection zooKeeper;
		private volatile boolean closed = false;

    public static HConnection createConnection(Configuration conf) throws IOException {
        // TODO: implement ExecutorService "pool" to allow HBase to manage our connections?
        // HConnection connection = HConnectionManager.createConnection(conf, pool);
        // See: "HTablePool Deprecated in CDH 5" under http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH5-Release-Notes/cdh5rn_topic_3_5.html
        HConnection connection = new SpliceHConnection(conf, true);
        return connection;
    }


//        HConnectionManager.HConnectionKey connectionKey = new HConnectionManager.HConnectionKey(conf);
//        synchronized (HConnectionManager.HBASE_INSTANCES) {
//            HConnection connection = HConnectionManager.HBASE_INSTANCES.get(connectionKey);
//            if (connection == null) {
//                connection = new SpliceHConnection(conf, true);
//                HConnectionManager.HBASE_INSTANCES.put(connectionKey, (HConnectionManager.HConnectionImplementation) connection);
//            }
//								} else if (connection.isClosed()) {
//										HConnectionManager.deleteConnection(connectionKey, true);
//										connection = new HConnectionImplementation(conf, true);
//										HBASE_INSTANCES.put(connectionKey, connection);
//								}
//								connection.incCount();
//            return connection;
//        }
//    }

//    public static HConnection createConnection(Configuration conf) throws IOException {
//        HConnectionManager.HConnectionKey connectionKey = new HConnectionManager.HConnectionKey(conf);
//        synchronized (HConnectionManager.HBASE_INSTANCES) {
//            HConnection connection = HConnectionManager.HBASE_INSTANCES.get(connectionKey);
//            if (connection == null) {
//                connection = new SpliceHConnection(conf, true);
//                HConnectionManager.HBASE_INSTANCES.put(connectionKey, (HConnectionManager.HConnectionImplementation) connection);
//            }
////								} else if (connection.isClosed()) {
////										HConnectionManager.deleteConnection(connectionKey, true);
////										connection = new HConnectionImplementation(conf, true);
////										HBASE_INSTANCES.put(connectionKey, connection);
////								}
////								connection.incCount();
//            return connection;
//        }
//    }

    /**
     * constructor
     *
     * @param conf Configuration object
     */
    public SpliceHConnection(Configuration conf, boolean managed) throws IOException {
        super(conf, managed);
    }

    @Override
    public synchronized ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher() throws ZooKeeperConnectionException {
        if (zooKeeper == null) {
            if (this.isClosed()) {
                try {
                    throw new IOException(toString() + " closed");
                } catch (IOException e) {
                    throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper", e);
                }
            }
            try {
                zooKeeper = super.getKeepAliveZooKeeperWatcher();
            } catch (IOException e) {
                throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper", e);
            }
        }
        return zooKeeper;
    }

//    @Override
//    public synchronized ZooKeeperWatcher getZooKeeperWatcher() throws ZooKeeperConnectionException {
//        if (zooKeeper == null) {
//            if (this.isClosed()) {
//                try {
//                    throw new IOException(toString() + " closed");
//                } catch (IOException e) {
//                    throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper", e);
//                }
//            }
//            zooKeeper = ZkUtils.getZooKeeperWatcher();
//        }
//        return zooKeeper;
//    }

    @Override
		public boolean isClosed() {
				return false;
		}

		@Override
		public void close() {
				//no-op
		}
}
