/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.test.framework;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author Scott Fines
 *         Date: 6/21/16
 */
public class RuledConnection extends TestWatcher implements Connection{
    private static final TestConnectionPool connPool = new TestConnectionPool();
    private TestConnection delegate;
    private final String userName;
    private final String password;
    private final boolean autoCommit;
    private final String schema;

    public RuledConnection(String schema){
        this(schema,true);
    }

    public RuledConnection(String schema,boolean autoCommit){
        this(SpliceNetConnection.DEFAULT_USER,
                SpliceNetConnection.DEFAULT_USER_PASSWORD,
                schema,
                autoCommit);
    }

    public RuledConnection(String userName,String password){
        this(userName,password,null,true);
    }

    public RuledConnection(String userName,String password,boolean autoCommit){
        this(userName,password,null,autoCommit);
    }

    public RuledConnection(String userName,String password,String schema,boolean autoCommit){
        this.userName=userName;
        this.password=password;
        this.schema = processSchemaName(schema);
        this.autoCommit = autoCommit;
    }


    @Override
    protected void starting(Description description){
        try{
            if(delegate==null || delegate.isClosed())
                delegate = getNewConnection();
            if(!autoCommit)
                delegate.setAutoCommit(false);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void finished(Description description){
        if(!autoCommit && delegate!=null){
            try{
                if(!delegate.isClosed()){
                    delegate.rollback();
                    delegate.reset();
                }
            }catch(SQLException se){
                throw new RuntimeException(se);
            }
        }
    }

    @Override
    public Statement createStatement() throws SQLException{
        return delegate.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException{
        return delegate.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException{
        return delegate.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException{
        return delegate.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException{
        delegate.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException{
        return delegate.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException{
        delegate.commit();
    }

    @Override
    public void rollback() throws SQLException{
        delegate.rollback();
    }

    @Override
    public void close() throws SQLException{
        delegate.close();
    }

    public void reset() throws SQLException{
        delegate.reset();
    }

    @Override
    public boolean isClosed() throws SQLException{
        return delegate.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException{
        return delegate.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException{
        delegate.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException{
        return delegate.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException{
        delegate.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException{
        return delegate.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException{
        delegate.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException{
        return delegate.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException{
        delegate.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType,int resultSetConcurrency) throws SQLException{
        return delegate.createStatement(resultSetType,resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        return delegate.prepareStatement(sql,resultSetType,resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency) throws SQLException{
        return delegate.prepareCall(sql,resultSetType,resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException{
        return delegate.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException{
        delegate.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException{
        delegate.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException{
        return delegate.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException{
        return delegate.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException{
        return delegate.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException{
        delegate.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException{
        delegate.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        return delegate.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        return delegate.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql,int resultSetType,int resultSetConcurrency,int resultSetHoldability) throws SQLException{
        return delegate.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int autoGeneratedKeys) throws SQLException{
        return delegate.prepareStatement(sql,autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql,int[] columnIndexes) throws SQLException{
        return delegate.prepareStatement(sql,columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql,String[] columnNames) throws SQLException{
        return delegate.prepareStatement(sql,columnNames);
    }

    @Override
    public Clob createClob() throws SQLException{
        return delegate.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException{
        return delegate.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException{
        return delegate.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException{
        return delegate.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException{
        return delegate.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name,String value) throws SQLClientInfoException{
        delegate.setClientInfo(name,value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException{
        delegate.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException{
        return delegate.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException{
        return delegate.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName,Object[] elements) throws SQLException{
        return delegate.createArrayOf(typeName,elements);
    }

    @Override
    public Struct createStruct(String typeName,Object[] attributes) throws SQLException{
        return delegate.createStruct(typeName,attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException{
        delegate.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException{
        return delegate.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException{
        delegate.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor,int milliseconds) throws SQLException{
        delegate.setNetworkTimeout(executor,milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException{
        return delegate.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return delegate.isWrapperFor(iface);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private TestConnection getNewConnection() throws Exception {
        TestConnection conn = connPool.getConnection(userName,password);
        if(schema!=null)
            conn.setSchema(schema);
        return conn;
    }

    private String processSchemaName(String schema){
        if(schema==null) return null;
        if(schema.contains("\"")) return schema;
        else return schema.toUpperCase();
    }
}
