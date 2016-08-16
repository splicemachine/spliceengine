/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.iapi.reference.SQLState;

import java.sql.*;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
class ClusteredConnection implements Connection,Debuggable{
    private static final Logger LOGGER=Logger.getLogger(ClusteredConnection.class.getName());
    private final ClusteredConnManager connectionManager;
    private final boolean closeDataSourceOnClose;
    private boolean closed = false;
    private final String url;

    ClusteredConnection(String connectUrl,
                        ClusteredDataSource dataSource,
                        boolean closeDataSourceOnClose,
                        Properties connectionproperties){
        this.url = connectUrl;
        this.connectionManager = new ClusteredConnManager(dataSource);
        this.closeDataSourceOnClose = closeDataSourceOnClose;
        if(LOGGER.isLoggable(Level.FINEST)){
            LOGGER.finest("New Clustered Connection created using url "+connectUrl+", dataSource="+dataSource);
            LOGGER.finest("Pooled connections: "+dataSource.connectionCount());
        }
    }

    @Override
    public void logDebugInfo(Level logLevel){
        connectionManager.logDebugInfo(logLevel);
    }

    @Override
    public Statement createStatement() throws SQLException{
        return createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException{
        return prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException{
        return prepareCall(sql,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    @SuppressWarnings("MagicConstant") //getHoldability() will return the correct value, the linter just can't tell
    public Statement createStatement(int resultSetType,int resultSetConcurrency) throws SQLException{
        //use the current connection's default holdability
        return createStatement(resultSetType, resultSetConcurrency,getHoldability());
    }

    @Override
    @SuppressWarnings("MagicConstant") //getHoldability() will return the correct value, the linter just can't tell
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        return prepareStatement(sql,resultSetType,resultSetConcurrency,getHoldability());
    }

    @Override
    @SuppressWarnings("MagicConstant") //getHoldability() will return the correct value, the linter just can't tell
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        return prepareCall(sql,resultSetType,resultSetConcurrency,getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        checkClosed();
        ClusteredStatement cs = new ClusteredStatement(this,connectionManager,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                connectionManager.maxExecutionRetry());
        connectionManager.registerStatement(cs);
        return cs;
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        checkClosed();
        ClusteredPreparedStatement cps = new ClusteredPreparedStatement(this,connectionManager,
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                connectionManager.maxExecutionRetry());
        connectionManager.registerStatement(cps);
        return cps;
    }

    @Override
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        checkClosed();
        ClusteredCallableStatement cps = new ClusteredCallableStatement(this,connectionManager,
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                connectionManager.maxExecutionRetry());

        connectionManager.registerStatement(cps);
        return cps;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException{
        return new ClusteredMetaData(this,url,connectionManager,connectionManager.maxExecutionRetry());
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int autoGeneratedKeys) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int[] columnIndexes) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public PreparedStatement prepareStatement(String sql,String[] columnNames) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void close() throws SQLException{
        if(closed) return;
        if(LOGGER.isLoggable(Level.FINEST))
            LOGGER.finest("Closing clustered connection");
        connectionManager.close(closeDataSourceOnClose);
        closed=true;
    }

    @Override
    public boolean isClosed() throws SQLException{
        return closed;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException{
        checkClosed();
        return sql;
    }

    @Override
    public void commit() throws SQLException{
        checkClosed();
        connectionManager.commit();
    }

    @Override
    public void rollback() throws SQLException{
        checkClosed();
        connectionManager.rollback();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException{
        checkClosed();
        RefCountedConnection rcc = connectionManager.acquireConnection();
        rcc.acquire();
        Savepoint baseSp = rcc.element().setSavepoint();
        return new ClusteredSavepoint(rcc,baseSp);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException{
        checkClosed();
        RefCountedConnection rcc = connectionManager.acquireConnection();
        rcc.acquire();
        Savepoint baseSp = rcc.element().setSavepoint();
        return new ClusteredSavepoint(rcc,baseSp);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException{
        checkClosed();
        assert savepoint instanceof ClusteredSavepoint: "Incorrect savepoint type!";
        ClusteredSavepoint cs = (ClusteredSavepoint)savepoint;
        cs.rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException{
        checkClosed();
        assert savepoint instanceof ClusteredSavepoint: "Incorrect savepoint type!";
        ClusteredSavepoint cs = (ClusteredSavepoint)savepoint;
        cs.commit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException{
        checkClosed();
        connectionManager.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException{
        checkClosed();
        return connectionManager.isAutoCommit();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException{
        checkClosed();
        if(connectionManager.inActiveTransaction())
            throw new SQLException("Cannot set Read only during active transaction",SQLState.AUTH_SET_CONNECTION_READ_ONLY_IN_ACTIVE_XACT);
        connectionManager.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException{
        checkClosed();
        return connectionManager.isReadOnly();
    }

    //SpliceMachine does not support catalogs
    @Override public void setCatalog(String catalog) throws SQLException{ }
    @Override public String getCatalog() throws SQLException{ return null; }

    @Override
    public void setTransactionIsolation(int level) throws SQLException{
        checkClosed();
        connectionManager.setIsolationLevel(TxnIsolation.parse(level));
    }

    @Override
    public int getTransactionIsolation() throws SQLException{
        checkClosed();
        return connectionManager.getIsolationLevel().level;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException{
        checkClosed();
        switch(holdability){
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                connectionManager.setHoldability(holdability);
            default:
                throw new SQLFeatureNotSupportedException("holdability ("+holdability+")",SQLState.UNSUPPORTED_HOLDABILITY_PROPERTY);
        }
    }

    @Override
    public int getHoldability() throws SQLException{
        checkClosed();
        return connectionManager.getHoldability();
    }

    @Override
    public void setSchema(String schema) throws SQLException{
        checkClosed();
        connectionManager.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException{
        checkClosed();
        return connectionManager.getSchema();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException{
        throw new SQLFeatureNotSupportedException("getTypeMap",SQLState.NOT_IMPLEMENTED);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException{
        throw new SQLFeatureNotSupportedException("getTypeMap",SQLState.NOT_IMPLEMENTED);
    }

    @Override
    public void setClientInfo(String name,String value) throws SQLClientInfoException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public String getClientInfo(String name) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Properties getClientInfo() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        return null; //TODO -sf- implement
    }

    @Override
    public void clearWarnings() throws SQLException{
    }

    @Override
    public Clob createClob() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Blob createBlob() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public NClob createNClob() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Array createArrayOf(String typeName,Object[] elements) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Struct createStruct(String typeName,Object[] attributes) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException{
        if(timeout<0)
            throw new SQLException("invalid timeout",SQLState.INVALID_QUERYTIMEOUT_VALUE);
        return connectionManager.validate(timeout);
    }

    @Override
    public void abort(Executor executor) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setNetworkTimeout(Executor executor,int milliseconds) throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public int getNetworkTimeout() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLException("Cannot unwrap iface <"+iface+">",SQLState.UNABLE_TO_UNWRAP);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        //we don't wrap anything
        return false;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void checkClosed() throws SQLException{
        if(closed)
            throw new SQLException("Connection closed",SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED);
    }
}
