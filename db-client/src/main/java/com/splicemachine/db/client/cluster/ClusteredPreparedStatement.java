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

import com.splicemachine.db.client.am.ClientMessageId;
import com.splicemachine.db.client.am.ColumnMetaData;
import com.splicemachine.db.client.am.SQLExceptionFactory;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.iapi.reference.SQLState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
class ClusteredPreparedStatement extends ClusteredStatement implements PreparedStatement{
    final String baseSql;
    protected transient PreparedStatement delegate;
    protected StatementParameters params;
    List<StatementParameters> batchParams;

    ClusteredPreparedStatement(ClusteredConnection sourceConnection,
                               ClusteredConnManager connManager,
                               String baseSql,
                               int resultSetType,
                               int resultSetConcurrency,
                               int resultSetHoldability,
                               int executionRetries) throws SQLException{
        super(sourceConnection,connManager,resultSetType,resultSetConcurrency,resultSetHoldability,executionRetries);
        this.baseSql = baseSql;

        reopenIfNecessary();
    }

    @Override
    protected Statement newStatement(Connection delegateConnection) throws SQLException{
        SQLException error = null;
        int numTries = executionRetries;
        while(numTries>0){
            try{
                delegate=delegateConnection.prepareStatement(baseSql);
                if(params==null)
                    params = new StatementParameters(delegate.getParameterMetaData().unwrap(ColumnMetaData.class));
                return delegate;
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(ClientErrors.isNetworkError(se) && params.canRetry()){
                    disconnect();
                }else throw error;
            }
        }
        throw error;
    }

    @Override
    public ResultSet executeQuery() throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException error = null;
        while(numTries>0){
            reopenIfNecessary();
            PreparedStatement ps = delegate;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            try{
                return ps.executeQuery();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(ClientErrors.isNetworkError(se) && params.canRetry() && connManager.isAutoCommit()){
                    disconnect();
                }else throw error;
            }
        }
        throw error;
    }

    @Override
    public int executeUpdate() throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException error = null;
        while(numTries>0){
            reopenIfNecessary();
            PreparedStatement ps = delegate;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            try{
                return ps.executeUpdate();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(ClientErrors.isNetworkError(se) && params.canRetry() && connManager.isAutoCommit()){
                    disconnect();
                }else throw error;
            }
        }
        throw error;
    }

    @Override
    public void clearParameters() throws SQLException{
        params.reset();
    }

    @Override
    public boolean execute() throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException error = null;
        while(numTries>0){
            reopenIfNecessary();
            PreparedStatement ps = delegate;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            try{
                return ps.execute();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(ClientErrors.isNetworkError(se) && params.canRetry() && connManager.isAutoCommit()){
                    disconnect();
                }else throw error;
            }
            numTries--;
        }
        throw error;
    }


    @Override
    public void addBatch() throws SQLException{
        checkClosed();
        reopenIfNecessary();
        if(batchParams==null)
            batchParams = new ArrayList<>();

        batchParams.add(new StatementParameters(params));
    }

    @Override
    public int[] executeBatch() throws SQLException{
        checkClosed();
        if(batchParams==null) return new int[]{};
        SQLException error = null;
        int numTries = executionRetries;
        while(numTries>0){
            reopenIfNecessary();
            PreparedStatement ps = delegate;
            for(StatementParameters sp:batchParams){
                sp.setParameters(ps);
                ps.addBatch();
            }
            try{
                int[] retCodes=ps.executeBatch();
                /*
                 * Since we were able to execute without error, go ahead and
                 * clear the batch parameters here.
                 */
                batchParams.clear();
                return retCodes;
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(ClientErrors.isNetworkError(se) && params.canRetry() && connManager.isAutoCommit()){
                    disconnect();
                }else {
                    //if we get an error that we can't deal with, clear the batch
                    batchParams.clear();
                    throw error;
                }
            }
            numTries--;
        }
        throw error;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException{
        checkClosed();
        reopenIfNecessary();
        return delegate.getParameterMetaData();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException{
        checkClosed();
        reopenIfNecessary();
        return delegate.getMetaData();
    }

    @Override
    public void setNull(int parameterIndex,int sqlType) throws SQLException{
        params.setNull(parameterIndex,sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex,boolean x) throws SQLException{
        params.setBoolean(parameterIndex,x);
    }

    @Override
    public void setByte(int parameterIndex,byte x) throws SQLException{
        params.setByte(parameterIndex,x);

    }

    @Override
    public void setShort(int parameterIndex,short x) throws SQLException{
        params.setShort(parameterIndex,x);

    }

    @Override
    public void setInt(int parameterIndex,int x) throws SQLException{
        params.setInt(parameterIndex,x);

    }

    @Override
    public void setLong(int parameterIndex,long x) throws SQLException{
        params.setLong(parameterIndex,x);

    }

    @Override
    public void setFloat(int parameterIndex,float x) throws SQLException{
        params.setFloat(parameterIndex,x);

    }

    @Override
    public void setDouble(int parameterIndex,double x) throws SQLException{
        params.setDouble(parameterIndex,x);

    }

    @Override
    public void setBigDecimal(int parameterIndex,BigDecimal x) throws SQLException{
        params.setBigDecimal(parameterIndex,x);

    }

    @Override
    public void setString(int parameterIndex,String x) throws SQLException{
        params.setString(parameterIndex,x);

    }

    @Override
    public void setBytes(int parameterIndex,byte[] x) throws SQLException{
        params.setBytes(parameterIndex,x);

    }

    @Override
    public void setDate(int parameterIndex,Date x) throws SQLException{
        params.setDate(parameterIndex,x);

    }

    @Override
    public void setTime(int parameterIndex,Time x) throws SQLException{
        params.setTime(parameterIndex,x);

    }

    @Override
    public void setTimestamp(int parameterIndex,Timestamp x) throws SQLException{
        params.setTimestamp(parameterIndex,x);

    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x,int length) throws SQLException{
        params.setAsciiStream(parameterIndex,x,length);

    }

    @Override
    public void setUnicodeStream(int parameterIndex,InputStream x,int length) throws SQLException{
        params.setUnicodeStream(parameterIndex,x,length);
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x,int length) throws SQLException{
        params.setBinaryStream(parameterIndex,x,length);
    }


    @Override
    public void setObject(int parameterIndex,Object x,int targetSqlType) throws SQLException{
        params.setObject(parameterIndex,x,targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex,Object x) throws SQLException{
        params.setObject(parameterIndex,x);

    }


    @Override
    public void setCharacterStream(int parameterIndex,Reader reader,int length) throws SQLException{
        params.setCharacterStream(parameterIndex,reader,length);

    }

    @Override
    public void setRef(int parameterIndex,Ref x) throws SQLException{
        methodNotSupported();
    }

    @Override
    public void setBlob(int parameterIndex,Blob x) throws SQLException{
        params.setBlob(parameterIndex,x);

    }

    @Override
    public void setClob(int parameterIndex,Clob x) throws SQLException{
        params.setClob(parameterIndex,x);

    }

    @Override
    public void setArray(int parameterIndex,Array x) throws SQLException{
        methodNotSupported();
    }

    @Override
    public void setDate(int parameterIndex,Date x,Calendar cal) throws SQLException{
        params.setDate(parameterIndex,x,cal);
    }

    @Override
    public void setTime(int parameterIndex,Time x,Calendar cal) throws SQLException{
        params.setTime(parameterIndex,x,cal);

    }

    @Override
    public void setTimestamp(int parameterIndex,Timestamp x,Calendar cal) throws SQLException{
        params.setTimestamp(parameterIndex,x,cal);
    }

    @Override
    public void setNull(int parameterIndex,int sqlType,String typeName) throws SQLException{
        methodNotSupported();
    }


    @Override
    public void setURL(int parameterIndex,URL x) throws SQLException{
        methodNotSupported();
    }

    @Override
    public void setRowId(int parameterIndex,RowId x) throws SQLException{
        params.setRowId(parameterIndex,x);
    }

    @Override
    public void setClob(int parameterIndex,Reader reader,long length) throws SQLException{
        params.setClob(parameterIndex,reader,length);
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream,long length) throws SQLException{
        params.setBlob(parameterIndex,inputStream,length);
    }

    @Override
    public void setObject(int parameterIndex,Object x,int targetSqlType,int scaleOrLength) throws SQLException{
        params.setObject(parameterIndex,x,targetSqlType,scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x) throws SQLException{
        params.setAsciiStream(parameterIndex,x,-1L);
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x,long length) throws SQLException{
        params.setAsciiStream(parameterIndex,x,length);
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x) throws SQLException{
        params.setBinaryStream(parameterIndex,x,-1L);
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x,long length) throws SQLException{
        params.setBinaryStream(parameterIndex,x,length);
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader,long length) throws SQLException{
        params.setCharacterStream(parameterIndex,reader,length);
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader) throws SQLException{
        params.setCharacterStream(parameterIndex,reader);
    }

    @Override
    public void setClob(int parameterIndex,Reader reader) throws SQLException{
        params.setClob(parameterIndex,reader);
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream) throws SQLException{
        params.setBlob(parameterIndex,inputStream);
    }

    @Override
    public void setNString(int index, String value) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setNString (int, String)");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNCharacterStream(int,Reader)");
    }

    @Override
    public void setNCharacterStream(int index, Reader value, long length) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setNCharacterStream(int,Reader,long)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw SQLExceptionFactory.notImplemented("setNClob(int,Reader)");
    }

    @Override
    public void setNClob(int index, NClob value) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setNClob (int, NClob)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setNClob (int, Reader, long)");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException{
        throw SQLExceptionFactory.notImplemented ("setSQLXML (int, SQLXML)");
    }

    @Override
    public void addBatch(String sql) throws SQLException{
        throw notForPreparedStatement("addBatch(sql)");
    }

    @Override
    public boolean execute(String sql,int autoGeneratedKeys) throws SQLException{
        throw notForPreparedStatement("execute(String, int)");
    }

    @Override
    public boolean execute(String sql,int[] columnIndexes) throws SQLException{
        throw notForPreparedStatement("execute(String, int[])");
    }

    @Override
    public boolean execute(String sql,String[] columnNames) throws SQLException{
        throw notForPreparedStatement("execute(String, String[])");
    }

    @Override
    public boolean execute(String sql) throws SQLException{
        throw notForPreparedStatement("execute(String)");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException{
        throw notForPreparedStatement("executeQuery(String)");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException{
        throw notForPreparedStatement("executeUpdate(String)");
    }

    @Override
    public int executeUpdate(String sql,int autoGeneratedKeys) throws SQLException{
        throw notForPreparedStatement("executeUpdate(String,int)");
    }

    @Override
    public int executeUpdate(String sql,int[] columnIndexes) throws SQLException{
        throw notForPreparedStatement("executeUpdate(String,int[])");
    }

    @Override
    public int executeUpdate(String sql,String[] columnNames) throws SQLException{
        throw notForPreparedStatement("executeUpdate(String,String[])");
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private SQLException notForPreparedStatement(String op){
        return new SqlException(null,new ClientMessageId(SQLState.NOT_FOR_PREPARED_STATEMENT),op).getSQLException();
    }

    private void methodNotSupported() throws SQLException{
        throw new SqlException(null,new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED)).getSQLException();
    }

}
