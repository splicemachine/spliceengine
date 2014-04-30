package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.security.UserProvider;

/**
 * @author Scott Fines
 *         Date: 11/20/13
 */
public class SpliceHConnection extends HConnectionManager.HConnectionImplementation {
    private static ZooKeeperKeepAliveConnection zooKeeper;
    private volatile boolean closed = false;

    public SpliceHConnection(Configuration config, ExecutorService connectionPool) throws IOException {
        super(config, false, connectionPool, UserProvider.instantiate(config).getCurrent());
    }

    @Override
    public synchronized ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher() throws
        ZooKeeperConnectionException {
        if (zooKeeper == null) {
            if (this.isClosed()) {
                try {
                    throw new IOException(this.toString() + " closed");
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

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        super.close();
    }
}
