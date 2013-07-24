package com.splicemachine.derby.nist;

import java.sql.Connection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Jeff Cunningham
 *         Date: 7/23/13
 */
public class SimpleConnectionPool implements ConnectionPool {

    private Queue<Connection> pool = new ConcurrentLinkedQueue<Connection>();
    private final ConnectionFactory factory;

    public SimpleConnectionPool(ConnectionFactory factory) throws Exception {
        this.factory = factory;
        newPooledConnection();
    }

    @Override
    public synchronized Connection getConnection() throws Exception {

        Connection connection = pool.poll();
        if (connection == null) {
            connection = createConnection();
        }
        if (connection == null) {
            throw new Exception("Can't create any more Connections with factory "+this.factory.getClass().getSimpleName());
        }
        if (connection.isClosed()) {
            connection = createConnection();
        }
        return connection;
    }

    @Override
    public synchronized void returnConnection(Connection connection) throws Exception {
        if (connection != null  && ! connection.isClosed()) {
            pool.offer(connection);
        }
    }

    private Connection createConnection() throws Exception {
        newPooledConnection();

        return pool.poll();
    }

    private void newPooledConnection() throws Exception {
        Connection connection = factory.getConnection();
        pool.offer(connection);
    }
}
