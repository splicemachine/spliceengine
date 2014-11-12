package com.splicemachine.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * A Pool of Connections.
 *
 * To get a Connection, call one of the methods
 * {@link #acquire()},{@link #tryAcquire()}, or {@link #tryAcquire(long, java.util.concurrent.TimeUnit)}, depending
 * on the level of blocking you desire. Then, once finished with the Connection, simply call {@code close()} on the
 * connection itself and it will be returned to the pool.
 *
 * @author Scott Fines
 * Created on: 3/22/13
 */
public class ConnectionPool implements ConnectionPoolStatus {
    /*Default maximum connections created in this pool*/
    private static final int DEFAULT_MAX_CONNECTIONS = 100;
    /*Configuration string for adjusting the max connections up or down*/
    private static final String MAX_CONNECTIONS="splice.execution.maxconns";

    /**
     * A Supplier of Connections. Only called when a new connection is needed.
     */
    public interface Supplier{
        /**
         * Create a new connection
         * @return a new connection instance
         * @throws SQLException if something goes wrong creating that connection
         */
        Connection createNew() throws SQLException;
    }

    private final Supplier connectionMaker;
    private final DynamicSemaphore permits;
    private final BlockingQueue<Connection> alreadyCreatedConnections;

    private volatile boolean closed;

    private ConnectionPool(Supplier connectionMaker,
                           int poolSize,
                           boolean fairPool) {
        this.connectionMaker = connectionMaker;
        this.permits = new DynamicSemaphore(poolSize,fairPool);
        this.alreadyCreatedConnections = new ArrayBlockingQueue<Connection>(poolSize);
    }

    public static ConnectionPool create(Supplier connectionMaker,int poolSize){
        return new ConnectionPool(connectionMaker,poolSize,true);
    }

    public static ConnectionPool create(Configuration configuration){
        int connectionPoolSize = configuration.getInt(MAX_CONNECTIONS,DEFAULT_MAX_CONNECTIONS);

        Supplier supplier = new EmbedConnectionMaker();

        return new ConnectionPool(supplier,connectionPoolSize,false);
    }

    /**
     * Acquires a Connection from the pool, waiting if necessary until one becomes available.
     *
     * This method will block until a connection becomes available, it will <em>not</em> fail to return a
     * connection, but it will also <em>not</em> back off from attempting acquisition unless interrupted.
     *
     * @return a Connection
     * @throws InterruptedException if interrupted while attempting to get a connection from the pool.
     * @throws SQLException if something goes wrong getting the connection
     */
    public Connection acquire() throws InterruptedException, SQLException {
        Preconditions.checkArgument(!closed,"Pool is closed");
        permits.acquire();

        return getConnection();
    }

    /**
     * Try to acquire a connection if one becomes available before the specified timeout is reached.
     *
     * If a connection becomes available before {@code timeout} is reached, then it will be returned. Otherwise,
     * {@code null} is returned.
     *
     * @param timeout how long to wait before giving up on acquisition
     * @param unit the time unit to use for wait computations.
     * @return a Connection, of {@code null} if no connection became available within the specified time frame.
     * @throws InterruptedException if interrupted while waiting for a connection
     * @throws SQLException if something goes wrong getting a connection
     */
    public Connection tryAcquire(long timeout,TimeUnit unit) throws InterruptedException, SQLException {
        Preconditions.checkArgument(!closed,"Pool is closed");
        if(permits.tryAcquire(timeout,unit))
            return getConnection();
        return null;
    }

    /**
     * Try to acquire a connection, returning immediately if a connection is not immediately available.
     *
     * @return a Connection if one is already available in the pool, or {@code null} otherwise.
     *
     * @throws SQLException if something goes wrong acquiring the connection
     */
    public Connection tryAcquire() throws SQLException {
        Preconditions.checkArgument(!closed,"Pool is closed");
        if(permits.tryAcquire())
            return getConnection();
        return null;
    }

    /**
     * Shut down the pool. No further acquisitions will be allowed from the pool. Outstanding Connections
     * will be closed as they are returned to the pool, but will not be closed immediately.
     *
     * @throws SQLException If something goes wrong closing the pool
     */
    public void shutdown() throws SQLException {
        this.closed=true;
        SQLException error = null;
        for(Connection connection: alreadyCreatedConnections){
            try{
                connection.close();
            } catch (SQLException e) {
                error = e;
            }
        }
        //TODO -sf- this will wipe out all the other errors and just throw the last
        //we probably want something better than this
        if(error!=null)
            throw error;
    }

/**************************************************************************************************/
    /*JMX status methods*/

    @Override
    public int getWaiting(){
        return permits.getQueueLength();
    }

    @Override
    public int getMaxPoolSize(){
        return permits.getNumPermits();
    }

    @Override
    public void setMaxPoolSize(int newMaxPoolSize){
        permits.setMaxPermits(newMaxPoolSize);
    }

    @Override
    public int getAvailable(){
        return permits.availablePermits();
    }

    @Override
    public int getInUse(){
        return Math.max(0,permits.getNumPermits()-permits.availablePermits());
    }

/*****************************************************************************************************/
    /*private helper methods*/

    private Connection getConnection() throws SQLException {
        Connection conn = alreadyCreatedConnections.poll();
        if(conn!=null)
            return new PooledConnection(conn);

        //there are no pooled connections to re-use, so create a new one
        return createConnection();
    }

    private Connection createConnection() throws SQLException {
        return new PooledConnection(connectionMaker.createNew());
    }

    /*
     * Convenience wrapper for Connections.
     */
    private class PooledConnection implements Connection{
        private final Connection delegate;
        private boolean connClosed = false;

        private PooledConnection(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() throws SQLException {
            if (connClosed)
        	    return;
            connClosed=true;
            if(ConnectionPool.this.closed||!alreadyCreatedConnections.offer(delegate)) {
               /*
                * This should never happen, because we use a semaphore to bound the number of connections
                * that can be created, so at least in theory we should never have created more connections
                * than the pool can hold, so closing a pooled connection should always allow the delegate
                * to be returned.
                *
                * However, if there's a programming error, this will prevent accidental connection leaks
                * by closing any connections that are not allowed back into the pool.
                */
                try{
                    delegate.close();
                }finally{
                    /*
                     * If there's a problem closing the delegate, you still want to release your permit
                     * to allow others to get new connections
                     */
                    permits.release();
                }
            }else{
                /*
                 * We've successfully returned the delegate to the pool, so release your permit
                 * and allow another thread to get access to the connection.
                 */
                permits.release();
            }
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if(!isWrapperFor(iface)) throw new SQLException("Not a wrapper for Class "+ iface);
            return iface.cast(delegate);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return iface.isAssignableFrom(delegate.getClass());
        }

        @Override
        public boolean isClosed() throws SQLException {
            return connClosed;
        }

        @Override public Statement createStatement() throws SQLException { return delegate.createStatement(); }
        @Override public String nativeSQL(String sql) throws SQLException { return delegate.nativeSQL(sql); }
        @Override public void commit() throws SQLException { delegate.commit(); }
        @Override public void rollback() throws SQLException { delegate.rollback(); }
        @Override public DatabaseMetaData getMetaData() throws SQLException { return delegate.getMetaData(); }
        @Override public void setReadOnly(boolean readOnly) throws SQLException { delegate.setReadOnly(readOnly); }
        @Override public boolean isReadOnly() throws SQLException { return delegate.isReadOnly(); }
        @Override public void setCatalog(String catalog) throws SQLException { delegate.setCatalog(catalog); }
        @Override public String getCatalog() throws SQLException { return delegate.getCatalog(); }
        @Override public SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
        @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }
        @Override public Map<String, Class<?>> getTypeMap() throws SQLException { return delegate.getTypeMap(); }
        @Override public int getHoldability() throws SQLException { return delegate.getHoldability(); }
        @Override public Savepoint setSavepoint() throws SQLException { return delegate.setSavepoint(); }
        @Override public Savepoint setSavepoint(String name) throws SQLException {return delegate.setSavepoint(name);}
        @Override public void rollback(Savepoint savepoint) throws SQLException { delegate.rollback(savepoint); }
        @Override public Clob createClob() throws SQLException { return delegate.createClob(); }
        @Override public Blob createBlob() throws SQLException { return delegate.createBlob(); }
        @Override public NClob createNClob() throws SQLException { return delegate.createNClob(); }
        @Override public SQLXML createSQLXML() throws SQLException { return delegate.createSQLXML(); }
        @Override public boolean isValid(int timeout) throws SQLException { return delegate.isValid(timeout); }
        @Override public String getClientInfo(String name) throws SQLException { return delegate.getClientInfo(name); }
        @Override public Properties getClientInfo() throws SQLException { return delegate.getClientInfo(); }

        //Java 7 compliant java.sql.Connection
        public void setSchema(String schema) throws SQLException { throw new UnsupportedOperationException(); }
        public String getSchema() throws SQLException { throw new UnsupportedOperationException() ;}
        public void abort(Executor executor) throws SQLException { throw new UnsupportedOperationException(); }
        public int getNetworkTimeout() throws SQLException { throw new UnsupportedOperationException(); }
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            delegate.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return delegate.getAutoCommit();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return delegate.prepareStatement(sql);
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return delegate.prepareCall(sql);
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            delegate.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return delegate.getTransactionIsolation();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            delegate.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            delegate.setHoldability(holdability);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            delegate.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return delegate.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return delegate.prepareStatement(sql, columnIndexes);
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return delegate.prepareStatement(sql, columnNames);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            delegate.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            delegate.setClientInfo(properties);
        }


        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return delegate.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return delegate.createStruct(typeName, attributes);
        }



    }

    private static class DynamicSemaphore extends Semaphore{
        private volatile int numPermits;
        public DynamicSemaphore(int permits) {
            super(permits);
            this.numPermits = permits;
        }
        public DynamicSemaphore(int permits, boolean fair) {
            super(permits, fair);
            this.numPermits = permits;
        }

        public int getNumPermits(){
            return numPermits;
        }

        public synchronized void setMaxPermits(int newMax){
            Preconditions.checkArgument(newMax>0,"Cannot have a pool with fewer than 1 entry, "+ newMax+" specified");
            int change = newMax-numPermits;
            if(change>0){
                release(change);
            }else{
                reducePermits(Math.abs(change));
            }
        }
    }
}
