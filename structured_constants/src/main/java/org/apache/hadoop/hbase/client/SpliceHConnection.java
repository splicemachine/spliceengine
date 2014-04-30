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
        System.out.println("+++ Creating SpliceHConnection");
    }

    @Override
    public synchronized ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher() throws
        ZooKeeperConnectionException {
        System.out.println("+++ Zoo is "+(zooKeeper != null?(zooKeeper.isAborted()?"aborted":"alive"):"null"));
        if (zooKeeper == null) {
            if (this.isClosed()) {
                try {
                    throw new IOException(this.toString() + " closed");
                } catch (IOException e) {
                    throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper", e);
                }
            }
            try {
                System.out.println("+++ Creating new Zoo");
                zooKeeper = super.getKeepAliveZooKeeperWatcher();
            } catch (IOException e) {
                throw new ZooKeeperConnectionException("An error is preventing a connection to ZooKeeper", e);
            }
        }
        System.out.println("+++ returning zoo "+zooKeeper);
        return zooKeeper;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        System.out.println("+++ Closing SpliceHConnection "+this);
        closed = true;
        super.close();
    }
}
