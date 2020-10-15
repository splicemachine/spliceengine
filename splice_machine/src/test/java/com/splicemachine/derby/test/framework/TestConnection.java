/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import splice.com.google.common.collect.Lists;
import com.splicemachine.derby.test.ManagedCallableStatement;
import com.splicemachine.stream.Accumulator;
import com.splicemachine.stream.StreamException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 8/4/14
 */
public class TestConnection implements Connection{
    private final Connection delegate;

    private final List<Statement> statements = Lists.newArrayList();

    private boolean oldAutoCommit;
    private String oldSchema;

    public TestConnection(Connection delegate) throws SQLException {
        this.delegate = delegate;
        this.oldAutoCommit = delegate.getAutoCommit();
    }

    public ResultSet query(String sql) throws SQLException{
        Statement s = createStatement();
        return s.executeQuery(sql);
    }

    @Override
    public Statement createStatement() throws SQLException {
        Statement statement = delegate.createStatement();
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        CallableStatement callableStatement = delegate.prepareCall(sql);
        statements.add(callableStatement);
        return callableStatement;
    }

    @Override public String nativeSQL(String sql) throws SQLException { return delegate.nativeSQL(sql); }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        delegate.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return delegate.getAutoCommit();
    }

    @Override public void commit() throws SQLException { delegate.commit(); }
    @Override public void rollback() throws SQLException { delegate.rollback(); }

    @Override
    public void close() throws SQLException {
        closeStatements();
        rollback();
        delegate.close();
    }


    public void reset() throws SQLException {
        delegate.setAutoCommit(oldAutoCommit);
        if(oldSchema!=null)
            delegate.setSchema(oldSchema);
    }

    @Override public boolean isClosed() throws SQLException { return delegate.isClosed(); }
    @Override public DatabaseMetaData getMetaData() throws SQLException { return delegate.getMetaData(); }
    @Override public void setReadOnly(boolean readOnly) throws SQLException { delegate.setReadOnly(readOnly); }
    @Override public boolean isReadOnly() throws SQLException { return delegate.isReadOnly(); }
    @Override public void setCatalog(String catalog) throws SQLException { delegate.setCatalog(catalog); }
    @Override public String getCatalog() throws SQLException { return delegate.getCatalog(); }
    @Override public void setTransactionIsolation(int level) throws SQLException { delegate.setTransactionIsolation(level); }
    @Override public int getTransactionIsolation() throws SQLException { return delegate.getTransactionIsolation(); }
    @Override public SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
    @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement statement = delegate.createStatement(resultSetType, resultSetConcurrency);
        ManagedStatement ms = new ManagedStatement(statement);
        statements.add(ms);
        return ms;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql,resultSetType,resultSetConcurrency);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        CallableStatement callableStatement = delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
        callableStatement = new ManagedCallableStatement(callableStatement);
        statements.add(callableStatement);
        return callableStatement;
    }

    @Override public Map<String, Class<?>> getTypeMap() throws SQLException { return delegate.getTypeMap(); }
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException { delegate.setTypeMap(map); }
    @Override public void setHoldability(int holdability) throws SQLException { delegate.setHoldability(holdability); }
    @Override public int getHoldability() throws SQLException { return delegate.getHoldability(); }
    @Override public Savepoint setSavepoint() throws SQLException { return delegate.setSavepoint(); }
    @Override public Savepoint setSavepoint(String name) throws SQLException { return delegate.setSavepoint(name); }
    @Override public void rollback(Savepoint savepoint) throws SQLException { delegate.rollback(savepoint); }
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException { delegate.releaseSavepoint(savepoint); }
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        Statement statement = delegate.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
        statement = new ManagedStatement(statement);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        CallableStatement callableStatement = delegate.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
        callableStatement = new ManagedCallableStatement(callableStatement);
        statements.add(callableStatement);
        return callableStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql, autoGeneratedKeys);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql, columnIndexes);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql, columnNames);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override public Clob createClob() throws SQLException { return delegate.createClob(); }
    @Override public Blob createBlob() throws SQLException { return delegate.createBlob(); }
    @Override public NClob createNClob() throws SQLException { return delegate.createNClob(); }
    @Override public SQLXML createSQLXML() throws SQLException { return delegate.createSQLXML(); }
    @Override public boolean isValid(int timeout) throws SQLException { return delegate.isValid(timeout); }
    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException { delegate.setClientInfo(name, value); }
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException { delegate.setClientInfo(properties); }
    @Override public String getClientInfo(String name) throws SQLException { return delegate.getClientInfo(name); }
    @Override public Properties getClientInfo() throws SQLException { return delegate.getClientInfo(); }
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { return delegate.createArrayOf(typeName,elements); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { return delegate.createStruct(typeName, attributes); }
    public void setSchema(String schema) throws SQLException {
        oldSchema = delegate.getSchema();
        delegate.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return delegate.getSchema();
    }
    public void abort(Executor executor) throws SQLException {
        //no-op
    }
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
//        delegate.setNetworkTimeout(executor, milliseconds);
    }
    public int getNetworkTimeout() throws SQLException {
        return 0;
//        return delegate.getNetworkTimeout();
    }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { return delegate.unwrap(iface); }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return delegate.isWrapperFor(iface); }

    /*Convenience test methods*/

    public void forAllRows(String query,Accumulator<ResultSet>accumulator) throws Exception {
        try(Statement s = createStatement()){
            forAllRows(s,query,accumulator);
        }
    }

    public void forAllRows(Statement s,String query,Accumulator<ResultSet>accumulator) throws Exception {
        try(ResultSet resultSet = s.executeQuery(query)){
            while(resultSet.next()){
                accumulator.accumulate(resultSet);
            }
        }
    }

    public void collectStats(String schemaName,String tableName) throws SQLException{
        if(tableName==null){
            try(CallableStatement cs = prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                cs.setString(1,schemaName);
                cs.execute();
            }
        }else{
            try(CallableStatement cs = prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")){
                cs.setString(1,schemaName);
                cs.setString(2,tableName);
                cs.execute();
            }
        }
    }

    public long count(String query) throws Exception{
        final AtomicLong rowCount = new AtomicLong(0);
        forAllRows(query,new Accumulator<ResultSet>() {
            @Override
            public void accumulate(ResultSet next) throws StreamException {
                rowCount.incrementAndGet();
            }
        });
        return rowCount.get();
    }

    public long count(Statement s,String query) throws Exception{
        final AtomicLong rowCount = new AtomicLong(0);
        forAllRows(s,query,new Accumulator<ResultSet>() {
            @Override
            public void accumulate(ResultSet next) throws StreamException {
                rowCount.incrementAndGet();
            }
        });
        return rowCount.get();
    }

    public long[] getConglomNumbers(String schema,String table) throws SQLException{
        List<Long> congloms = new ArrayList<>();
        try(Statement s = createStatement()){
            String sql="select c.conglomeratenumber from "+
                    "sys.sysschemas s"+
                    ", sys.systables t"+
                    ", sys.sysconglomerates c "+
                    "where s.schemaid = t.schemaid "+
                    "and t.tableid = c.tableid ";
            if(schema!=null)
                sql += "and s.schemaname = '"+schema.toUpperCase()+"' ";
            if(table!=null)
                sql+="and t.tablename = '"+table.toUpperCase()+"' ";
            try(ResultSet rs = s.executeQuery(sql)){
               while(rs.next()){
                   congloms.add(rs.getLong(1));
               }
            }
        }
        long[] c = new long[congloms.size()];
        int i=0;
        for(Long l:congloms){
            c[i] = (l);
            i++;
        }
        return c;
    }

    /***********************************************************************************/
    /*private helper methods*/
    private void closeStatements() throws SQLException {
        for(Statement s:statements){
            if(s!=null)
                s.close();
        }
    }

    public long getCurrentTransactionId() throws SQLException {
        Statement s= createStatement();
        ResultSet resultSet = s.executeQuery("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        if(!resultSet.next())
            throw new IllegalStateException("Did not see any response from GET_CURRENT_TRANSACTION()");
        return resultSet.getLong(1);
    }

    public boolean execute(String sql) throws SQLException{
        Statement s = createStatement();
        return s.execute(sql);
    }
}
