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
import java.sql.Date;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 8/24/16
 */
class ClusteredCallableStatement extends ClusteredPreparedStatement implements CallableStatement{
    private OutputParameters outputParams;
    private CallableStatement delegateCall;
    private List<OutputParameters> batchOutputParams;

    ClusteredCallableStatement(ClusteredConnection sourceConnection,
                               ClusteredConnManager connManager,
                               String baseSql,
                               int resultSetType,
                               int resultSetConcurrency,
                               int resultSetHoldability,
                               int executionRetries) throws SQLException{
        super(sourceConnection,connManager,baseSql,resultSetType,resultSetConcurrency,resultSetHoldability,executionRetries);
    }

    @Override
    protected Statement newStatement(Connection delegateConnection) throws SQLException{
        delegateCall= delegateConnection.prepareCall(baseSql);
        if(outputParams==null)
            outputParams = new OutputParameters(delegateCall.getParameterMetaData());
        if(params==null)
            params = new StatementParameters(delegateCall.getParameterMetaData().unwrap(ColumnMetaData.class));
        else
            outputParams.reset(delegateCall.getParameterMetaData());
        super.delegate = delegateCall;
        return delegateCall;
    }

    @Override
    public ResultSet executeQuery() throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException error = null;
        while(numTries>0){
            reopenIfNecessary();
            CallableStatement ps = delegateCall;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            outputParams.setOutputParameters(delegateCall);
            try{
                return ps.executeQuery();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(!params.canRetry() || !ClientErrors.isNetworkError(se)){
                    throw error;
                }
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
            CallableStatement ps = delegateCall;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            outputParams.setOutputParameters(delegateCall);
            try{
                return ps.executeUpdate();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(!params.canRetry() || !ClientErrors.isNetworkError(se) ||!connManager.isAutoCommit()){
                    throw error;
                }
            }
        }
        throw error;
    }

    @Override
    public boolean execute() throws SQLException{
        checkClosed();
        int numTries = executionRetries;
        SQLException error = null;
        while(numTries>0){
            reopenIfNecessary();
            CallableStatement ps = delegateCall;
            params.setParameters(ps); //don't retry parameter errors, since they aren't networking
            outputParams.setOutputParameters(delegateCall);
            try{
                return ps.execute();
            }catch(SQLException se){
                if(error==null) error = se;
                else error.setNextException(se);
                if(!params.canRetry() || !ClientErrors.isNetworkError(se)||!connManager.isAutoCommit()){
                    throw error;
                }
            }
        }
        throw error;
    }

    @Override
    public void addBatch() throws SQLException{
        super.addBatch(); //do super parameter checking
        if(batchOutputParams==null)
            batchOutputParams = new ArrayList<>();

        batchOutputParams.add(new OutputParameters(outputParams));
    }

    @Override
    public int[] executeBatch() throws SQLException{
        checkClosed();
        if(batchParams==null) return new int[]{};
        SQLException error = null;
        int numTries = executionRetries;
        while(numTries>0){
            reopenIfNecessary();
            CallableStatement ps = delegateCall;
            if(batchParams!=null){
                if(batchOutputParams!=null){
                    assert batchParams.size() == batchOutputParams.size();
                    Iterator<StatementParameters> spIter = batchParams.iterator();
                    Iterator<OutputParameters> oIter = batchOutputParams.iterator();
                    while(spIter.hasNext()){
                        StatementParameters sp = spIter.next();
                        OutputParameters op = oIter.next();
                        sp.setParameters(ps);
                        op.setOutputParameters(ps);
                        ps.addBatch();
                    }
                }else{
                    for(StatementParameters sp : batchParams){
                        sp.setParameters(ps);
                        ps.addBatch();
                    }
                }
            }else if(outputParams!=null){
                for(OutputParameters op:batchOutputParams){
                    op.setOutputParameters(ps);
                    ps.addBatch();
                }
            }
            try{
                return ps.executeBatch();
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
    public void registerOutParameter(int parameterIndex,int sqlType) throws SQLException{
        outputParams.registerOutputParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(int parameterIndex,int sqlType,int scale) throws SQLException{
        outputParams.registerOutputParameter(parameterIndex, sqlType,scale);
    }

    @Override
    public void registerOutParameter(int parameterIndex,int sqlType,String typeName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType,int scale) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType,String typeName) throws SQLException{
        throw jdbcNotImplemented();
    }


    @Override
    public boolean wasNull() throws SQLException{
        checkClosed();
        return delegateCall.wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getString(parameterIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getBoolean(parameterIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getByte(parameterIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getShort(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getInt(parameterIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getLong(parameterIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getFloat(parameterIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getDouble(parameterIndex);
    }

    @Override
    @SuppressWarnings("deprecation") //deprecation necessary to fulfill the spec
    public BigDecimal getBigDecimal(int parameterIndex,int scale) throws SQLException{
        checkClosed();
        return delegateCall.getBigDecimal(parameterIndex,scale);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getBytes(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getDate(parameterIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getTime(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getTimestamp(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getObject(parameterIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getBigDecimal(parameterIndex);
    }

    @Override
    public Object getObject(int parameterIndex,Map<String, Class<?>> map) throws SQLException{
        checkClosed();
        return delegateCall.getObject(parameterIndex,map);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getRef(parameterIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getBlob(parameterIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getClob(parameterIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getArray(parameterIndex);
    }

    @Override
    public Date getDate(int parameterIndex,Calendar cal) throws SQLException{
        checkClosed();
        return delegateCall.getDate(parameterIndex,cal);
    }

    @Override
    public Time getTime(int parameterIndex,Calendar cal) throws SQLException{
        checkClosed();
        return delegateCall.getTime(parameterIndex,cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex,Calendar cal) throws SQLException{
        checkClosed();
        return delegateCall.getTimestamp(parameterIndex,cal);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getURL(parameterIndex);
    }

    @Override
    public void setURL(String parameterName,URL val) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void setNull(String parameterName,int sqlType) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void setBoolean(String parameterName,boolean x) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public void setByte(String parameterName,byte x) throws SQLException{
       throw  jdbcNotImplemented();
    }

    @Override
    public void setShort(String parameterName,short x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setInt(String parameterName,int x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setLong(String parameterName,long x) throws SQLException{

        throw jdbcNotImplemented();
    }

    @Override
    public void setFloat(String parameterName,float x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setDouble(String parameterName,double x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setBigDecimal(String parameterName,BigDecimal x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setString(String parameterName,String x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setBytes(String parameterName,byte[] x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setDate(String parameterName,Date x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setTime(String parameterName,Time x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setTimestamp(String parameterName,Timestamp x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x,int length) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x,int length) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setObject(String parameterName,Object x,int targetSqlType,int scale) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setObject(String parameterName,Object x,int targetSqlType) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setObject(String parameterName,Object x) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader,int length) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setDate(String parameterName,Date x,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setTime(String parameterName,Time x,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setTimestamp(String parameterName,Timestamp x,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public void setNull(String parameterName,int sqlType,String typeName) throws SQLException{
        throw jdbcNotImplemented();

    }

    @Override
    public String getString(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public byte getByte(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public short getShort(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public int getInt(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public long getLong(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public float getFloat(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public double getDouble(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Date getDate(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Time getTime(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Object getObject(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Object getObject(String parameterName,Map<String, Class<?>> map) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Array getArray(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Date getDate(String parameterName,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Time getTime(String parameterName,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public Timestamp getTimestamp(String parameterName,Calendar cal) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public URL getURL(String parameterName) throws SQLException{
        throw jdbcNotImplemented();
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException{
        throw SQLExceptionFactory.notSupported("getRowId(int)");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("getRowId(String)");
    }

    @Override
    public void setRowId(String parameterName,RowId x) throws SQLException{
        throw SQLExceptionFactory.notSupported("setRowId(String,RowId)");
    }

    @Override
    public void setNString(String parameterName,String value) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNString (String, String)");
    }

    @Override
    public void setNCharacterStream(String parameterName,Reader value,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNCharacterStream (String, String)");
    }

    @Override
    public void setNClob(String parameterName,NClob value) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNClob (String, NClob)");
    }

    @Override
    public void setClob(String parameterName,Reader reader,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setClob (String, Reader, long)");
    }

    @Override
    public void setBlob(String parameterName,InputStream inputStream,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBlob (String, InputStream, long)");
    }

    @Override
    public void setNClob(String parameterName,Reader reader,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNClob (String, Reader, long)");
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException{
        throw SQLExceptionFactory.notSupported("getNClob (int)");
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNClob (String)");
    }

    @Override
    public void setSQLXML(String parameterName,SQLXML xmlObject) throws SQLException{
        throw SQLExceptionFactory.notSupported("setSQLXML (String, SQLXML)");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException{
        throw SQLExceptionFactory.notSupported("getSQLXML (int)");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("getSQLXML (String)");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException{
        throw SQLExceptionFactory.notSupported("getNString (int)");
    }

    @Override
    public String getNString(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("getNString (String)");
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException{
        throw SQLExceptionFactory.notSupported("getNCharacterStream (int)");
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("getNCharacterStream (String)");
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException{
        checkClosed();
        return delegateCall.getCharacterStream(parameterIndex);
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException{
        throw SQLExceptionFactory.notSupported("getCharacterStream(String)");
    }

    @Override
    public void setBlob(String parameterName,Blob x) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBlob(String,Blob)");
    }

    @Override
    public void setClob(String parameterName,Clob x) throws SQLException{
        throw SQLExceptionFactory.notSupported("setClob(String,Clob)");
    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setAsciiStream(String,InputStream,long)");
    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBinaryStream(String,InputStream,long)");
    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader,long length) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBinaryStream(String,Reader,long)");
    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x) throws SQLException{
        throw SQLExceptionFactory.notSupported("setAsciiStream(String,InputStream)");
    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBinaryStream(String,InputStream)");
    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader) throws SQLException{
        throw SQLExceptionFactory.notSupported("setCharacterStream(String,Reader)");
    }

    @Override
    public void setNCharacterStream(String parameterName,Reader value) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNCharacterStream(String,Reader)");
    }

    @Override
    public void setClob(String parameterName,Reader reader) throws SQLException{
        throw SQLExceptionFactory.notSupported("setClob(String,Reader)");
    }

    @Override
    public void setBlob(String parameterName,InputStream inputStream) throws SQLException{
        throw SQLExceptionFactory.notSupported("setBlob(String,InputStream)");
    }

    @Override
    public void setNClob(String parameterName,Reader reader) throws SQLException{
        throw SQLExceptionFactory.notSupported("setNClob(String,Reader)");
    }

    @Override
    public <T> T getObject(int parameterIndex,Class<T> type) throws SQLException{
        checkClosed();
        return delegateCall.getObject(parameterIndex,type);
    }

    @Override
    public <T> T getObject(String parameterName,Class<T> type) throws SQLException{
        throw SQLExceptionFactory.notSupported("getObject(String,Class)");
    }

    /* ***************************************************************************************************************/
    /*private helper methods and classes*/
    private static class OutputParameters{
        private ParameterMetaData parameterMetaData;
        private boolean[] registeredParams;
        private int[] jdbcType;
        private int[] scale;

        OutputParameters(OutputParameters copy) throws SQLException{
            this.parameterMetaData = copy.parameterMetaData;
            int parameterCount=parameterMetaData.getParameterCount();
            this.registeredParams = Arrays.copyOf(copy.registeredParams,parameterCount);
            this.jdbcType = Arrays.copyOf(copy.jdbcType,parameterCount);
            this.scale = Arrays.copyOf(copy.scale,parameterCount);
        }

        OutputParameters(ParameterMetaData parameterMetaData) throws SQLException{
            this.parameterMetaData=parameterMetaData;
            int parameterCount=parameterMetaData.getParameterCount();
            this.registeredParams = new boolean[parameterCount];
            this.jdbcType = new int[parameterCount];
            this.scale = new int[parameterCount];
        }

        void reset(ParameterMetaData paramMetaData) throws SQLException{
            this.parameterMetaData = paramMetaData;
            int parameterCount=parameterMetaData.getParameterCount();
            if(parameterCount!=registeredParams.length){
                registeredParams=new boolean[parameterCount];
                jdbcType = new int[parameterCount];
                scale = new int[parameterCount];
            }else{
                Arrays.fill(registeredParams,false);
                Arrays.fill(jdbcType,0);
                Arrays.fill(scale,-1);
            }
        }


        void registerOutputParameter(int parameterIndex, int jdbcType) throws SQLException{
            validateParameterIndex(parameterIndex);
            registeredParams[parameterIndex-1] = true;
            this.jdbcType[parameterIndex-1] = jdbcType;
        }

        void registerOutputParameter(int parameterIndex, int jdbcType, int scale) throws SQLException{
            validateParameterIndex(parameterIndex);
            if(scale<0 || scale>31)
                throw new SqlException(null,
                        new ClientMessageId(SQLState.BAD_SCALE_VALUE),scale).getSQLException();

            registeredParams[parameterIndex-1] = true;
            this.jdbcType[parameterIndex-1] = jdbcType;
            this.scale[parameterIndex-1] = scale;
        }

        void setOutputParameters(CallableStatement delegate) throws SQLException{
            for(int i=0;i<registeredParams.length;i++){
                int s = this.scale[i];
                int jType = jdbcType[i];
                if(s>=0)
                    delegate.registerOutParameter(i+1,jType,s);
                else
                    delegate.registerOutParameter(i+1,jType);
            }
        }

        private void validateParameterIndex(int parameterIndex) throws SQLException{
            if(parameterIndex<0 || parameterIndex>registeredParams.length)
                throw new SqlException(null,
                        new ClientMessageId(SQLState.LANG_INVALID_PARAM_POSITION)).getSQLException();
        }

    }

    private SQLException jdbcNotImplemented() throws SQLException{
        return new SqlException(null,new ClientMessageId(SQLState.JDBC_METHOD_NOT_IMPLEMENTED)).getSQLException();
    }
}
