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

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A Forwarding Connection which wraps all calls in a try{}catch{} block,
 * in order to detect specific known errors and reports them using
 * the {@link #reportError(Throwable)} abstract method.
 *
 * @author Scott Fines
 *         Date: 8/15/16
 */
abstract class ErrorTrappingConnection implements Connection{
    protected final Connection delegate;

    ErrorTrappingConnection(Connection delegate){
        assert delegate!=null;
        this.delegate=delegate;
    }

    @Override
    public Statement createStatement() throws SQLException{
        try{
            return wrapStatement(delegate.createStatement());
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException{
        try{
            return wrapPrepare(delegate.prepareStatement(sql));
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }


    @Override
    public CallableStatement prepareCall(String sql) throws SQLException{
        try{
            return wrapCall(delegate.prepareCall(sql));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }


    @Override
    public String nativeSQL(String sql) throws SQLException{
        try{
            return delegate.nativeSQL(sql);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException{
        try{
            delegate.setAutoCommit(autoCommit);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException{
        try{
            return delegate.getAutoCommit();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void commit() throws SQLException{
        try{
            delegate.commit();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void rollback() throws SQLException{
        try{
            delegate.rollback();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void close() throws SQLException{
        try{
            delegate.close();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isClosed() throws SQLException{
        try{
            return delegate.isClosed();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException{
        try{
            return delegate.getMetaData();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException{
        try{
            delegate.setReadOnly(readOnly);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException{
        try{
            return delegate.isReadOnly();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCatalog(String catalog) throws SQLException{
        try{
            delegate.setCatalog(catalog);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getCatalog() throws SQLException{
        try{
            return delegate.getCatalog();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException{
        try{
            delegate.setTransactionIsolation(level);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException{
        try{
            return delegate.getTransactionIsolation();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        try{
            return delegate.getWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void clearWarnings() throws SQLException{
        try{
            delegate.clearWarnings();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Statement createStatement(int resultSetType,int resultSetConcurrency) throws SQLException{
        try{
            return  wrapStatement(delegate.createStatement(resultSetType,resultSetConcurrency));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        try{
            return  wrapPrepare(delegate.prepareStatement(sql,resultSetType,resultSetConcurrency));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        try{
            return wrapCall(delegate.prepareCall(sql,resultSetType,resultSetConcurrency));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException{
        try{
            return delegate.getTypeMap();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException{
        try{
            delegate.setTypeMap(map);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException{
        try{
            delegate.setHoldability(holdability);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getHoldability() throws SQLException{
        try{
            return delegate.getHoldability();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException{
        try{
            return delegate.setSavepoint();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException{
        try{
            return delegate.setSavepoint(name);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException{
        try{
            delegate.rollback(savepoint);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException{
        try{
            delegate.releaseSavepoint(savepoint);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Statement createStatement(int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        try{
            return wrapStatement(delegate.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        try{
            return wrapPrepare(delegate.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        try{
            return wrapCall(delegate.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int autoGeneratedKeys) throws SQLException{
        try{
            return wrapPrepare(delegate.prepareStatement(sql,autoGeneratedKeys));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int[] columnIndexes) throws SQLException{
        try{
            return wrapPrepare(delegate.prepareStatement(sql,columnIndexes));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql,String[] columnNames) throws SQLException{
        try{
            return wrapPrepare(delegate.prepareStatement(sql,columnNames));
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Clob createClob() throws SQLException{
        try{
            return delegate.createClob();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Blob createBlob() throws SQLException{
        try{
            return delegate.createBlob();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public NClob createNClob() throws SQLException{
        try{
            return delegate.createNClob();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException{
        try{
            return delegate.createSQLXML();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException{
        try{
            return delegate.isValid(timeout);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setClientInfo(String name,String value) throws SQLClientInfoException{
        try{
            delegate.setClientInfo(name,value);
        }catch(SQLClientInfoException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException{
        try{
            delegate.setClientInfo(properties);
        }catch(SQLClientInfoException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException{
        try{
            return delegate.getClientInfo(name);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException{
        try{
            return delegate.getClientInfo();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Array createArrayOf(String typeName,Object[] elements) throws SQLException{
        try{
            return delegate.createArrayOf(typeName,elements);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Struct createStruct(String typeName,Object[] attributes) throws SQLException{
        try{
            return delegate.createStruct(typeName,attributes);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException{
        try{
            delegate.setSchema(schema);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getSchema() throws SQLException{
        try{
            return delegate.getSchema();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException{
        try{
            delegate.abort(executor);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor,int milliseconds) throws SQLException{
        try{
            delegate.setNetworkTimeout(executor,milliseconds);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException{
        try{
            return delegate.getNetworkTimeout();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return delegate.isWrapperFor(iface);
    }

    protected abstract void reportError(Throwable t);

    protected abstract Statement wrapStatement(Statement statement);

    protected abstract PreparedStatement wrapPrepare(PreparedStatement preparedStatement);

    protected abstract CallableStatement wrapCall(CallableStatement callableStatement);
}
