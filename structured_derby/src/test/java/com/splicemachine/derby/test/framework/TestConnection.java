package com.splicemachine.derby.test.framework;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.ManagedCallableStatement;
import com.splicemachine.derby.utils.WarningState;
import com.splicemachine.stream.Accumulator;
import com.splicemachine.stream.StreamException;

import java.sql.*;
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
    private long statementId;

    public TestConnection(Connection delegate) throws SQLException {
        this.delegate = delegate;
        this.oldAutoCommit = delegate.getAutoCommit();
    }

    public ResultSet query(String sql) throws SQLException{
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(sql);
        statementId = parseWarnings(s.getWarnings());
        return rs;
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
        delegate.close();
    }


    public void reset() throws SQLException {
        delegate.setAutoCommit(oldAutoCommit);
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
        PreparedStatement preparedStatement = delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
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
        Statement statement = delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        statement = new ManagedStatement(statement);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        PreparedStatement preparedStatement = delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        preparedStatement = new ManagedPreparedStatement(preparedStatement);
        statements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        CallableStatement callableStatement = delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
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
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { return delegate.createArrayOf(typeName, elements); }
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { return delegate.createStruct(typeName, attributes); }
    public void setSchema(String schema) throws SQLException {

//        delegate.setSchema(schema);
    }
    public String getSchema() throws SQLException {
        return null;
//        return delegate.getSchema();
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
        Statement s = createStatement();
        ResultSet resultSet = s.executeQuery(query);
        SQLWarning warnings = s.getWarnings();
        statementId = parseWarnings(warnings);
        while(resultSet.next()){
            accumulator.accumulate(resultSet);
        }
    }

    private long parseWarnings(SQLWarning warnings) {
        if(warnings!=null){
            if(WarningState.XPLAIN_STATEMENT_ID.getSqlState().equals(warnings.getSQLState())){
                String message = warnings.getMessage();
                int start = message.indexOf("is ");
                return Long.parseLong(message.substring(start+3).replace(",",""));
            }else
                System.out.println(warnings.getMessage());
        }
        return -1l;
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

    public long getCount(PreparedStatement countPs) throws Exception{
        countPs.clearWarnings();
        ResultSet rs = countPs.executeQuery();
        SQLWarning warnings = countPs.getWarnings();
        statementId = parseWarnings(warnings);
        if(rs.next()) return rs.getLong(1);
        else return -1l;
    }

    public long getCount(String countQuery) throws Exception{
        assert countQuery.contains("count"): "query is not a count query!";
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(countQuery);
        SQLWarning warnings = s.getWarnings();
        statementId = parseWarnings(warnings);
        if(rs.next())
            return rs.getLong(1);
        else return -1;
    }

    public long getLastStatementId(){
        return statementId;
    }

    /***********************************************************************************/
    /*private helper methods*/
    private void closeStatements() throws SQLException {
        for(Statement s:statements){
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
        boolean executed = s.execute(sql);
        statementId = parseWarnings(s.getWarnings());
        return executed;
    }
}
