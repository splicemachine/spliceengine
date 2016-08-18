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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
 * @author Scott Fines
 *         Date: 8/17/16
 */
abstract class ErrorTrappingPreparedStatement extends ErrorTrappingStatement implements PreparedStatement{
    private PreparedStatement delegatePs;
    ErrorTrappingPreparedStatement(PreparedStatement delegate){
        super(delegate);
        this.delegatePs = delegate;
    }

    @Override
    public void setNClob(int parameterIndex,Reader reader) throws SQLException{
        try{
            delegatePs.setNClob(parameterIndex,reader);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream) throws SQLException{
        try{
            delegatePs.setBlob(parameterIndex,inputStream);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex,Reader reader) throws SQLException{
        try{
            delegatePs.setClob(parameterIndex,reader);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex,Reader value) throws SQLException{
        try{
            delegatePs.setNCharacterStream(parameterIndex,value);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader) throws SQLException{
        try{
            delegatePs.setCharacterStream(parameterIndex,reader);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x) throws SQLException{
        try{
            delegatePs.setBinaryStream(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x) throws SQLException{
        try{
            delegatePs.setAsciiStream(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader,long length) throws SQLException{
        try{
            delegatePs.setCharacterStream(parameterIndex,reader,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x,long length) throws SQLException{
        try{
            delegatePs.setBinaryStream(parameterIndex,x,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x,long length) throws SQLException{
        try{
            delegatePs.setAsciiStream(parameterIndex,x,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex,Object x,int targetSqlType,int scaleOrLength) throws SQLException{
        try{
            delegatePs.setObject(parameterIndex,x,targetSqlType,scaleOrLength);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setSQLXML(int parameterIndex,SQLXML xmlObject) throws SQLException{
        try{
            delegatePs.setSQLXML(parameterIndex,xmlObject);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex,Reader reader,long length) throws SQLException{
        try{
            delegatePs.setNClob(parameterIndex,reader,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream,long length) throws SQLException{
        try{
            delegatePs.setBlob(parameterIndex,inputStream,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex,Reader reader,long length) throws SQLException{
        try{
            delegatePs.setClob(parameterIndex,reader,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNClob(int parameterIndex,NClob value) throws SQLException{
        try{
            delegatePs.setNClob(parameterIndex,value);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex,Reader value,long length) throws SQLException{
        try{
            delegatePs.setNCharacterStream(parameterIndex,value,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNString(int parameterIndex,String value) throws SQLException{
        try{
            delegatePs.setNString(parameterIndex,value);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setRowId(int parameterIndex,RowId x) throws SQLException{
        try{
            delegatePs.setRowId(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException{
        try{
            return delegatePs.getParameterMetaData();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setURL(int parameterIndex,URL x) throws SQLException{
        try{
            delegatePs.setURL(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex,int sqlType,String typeName) throws SQLException{
        try{
            delegatePs.setNull(parameterIndex,sqlType,typeName);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex,Timestamp x,Calendar cal) throws SQLException{
        try{
            delegatePs.setTimestamp(parameterIndex,x,cal);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex,Time x,Calendar cal) throws SQLException{
        try{
            delegatePs.setTime(parameterIndex,x,cal);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex,Date x,Calendar cal) throws SQLException{
        try{
            delegatePs.setDate(parameterIndex,x,cal);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException{
        try{
            return delegatePs.getMetaData();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setArray(int parameterIndex,Array x) throws SQLException{
        try{
            delegatePs.setArray(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setClob(int parameterIndex,Clob x) throws SQLException{
        try{
            delegatePs.setClob(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBlob(int parameterIndex,Blob x) throws SQLException{
        try{
            delegatePs.setBlob(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setRef(int parameterIndex,Ref x) throws SQLException{
        try{
            delegatePs.setRef(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader,int length) throws SQLException{
        try{
            delegatePs.setCharacterStream(parameterIndex,reader,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void addBatch() throws SQLException{
        try{
            delegatePs.addBatch();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public boolean execute() throws SQLException{
        try{
            return delegatePs.execute();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex,Object x) throws SQLException{
        try{
            delegatePs.setObject(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setObject(int parameterIndex,Object x,int targetSqlType) throws SQLException{
        try{
            delegatePs.setObject(parameterIndex,x,targetSqlType);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void clearParameters() throws SQLException{
        try{
            delegatePs.clearParameters();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x,int length) throws SQLException{
        try{
            delegatePs.setBinaryStream(parameterIndex,x,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setUnicodeStream(int parameterIndex,InputStream x,int length) throws SQLException{
        try{
            delegatePs.setUnicodeStream(parameterIndex,x,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x,int length) throws SQLException{
        try{
            delegatePs.setAsciiStream(parameterIndex,x,length);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex,Timestamp x) throws SQLException{
        try{
            delegatePs.setTimestamp(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setTime(int parameterIndex,Time x) throws SQLException{
        try{
            delegatePs.setTime(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setDate(int parameterIndex,Date x) throws SQLException{
        try{
            delegatePs.setDate(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBytes(int parameterIndex,byte[] x) throws SQLException{
        try{
            delegatePs.setBytes(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setString(int parameterIndex,String x) throws SQLException{
        try{
            delegatePs.setString(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex,BigDecimal x) throws SQLException{
        try{
            delegatePs.setBigDecimal(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setDouble(int parameterIndex,double x) throws SQLException{
        try{
            delegatePs.setDouble(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setFloat(int parameterIndex,float x) throws SQLException{
        try{
            delegatePs.setFloat(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setLong(int parameterIndex,long x) throws SQLException{
        try{
            delegatePs.setLong(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setInt(int parameterIndex,int x) throws SQLException{
        try{
            delegatePs.setInt(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setShort(int parameterIndex,short x) throws SQLException{
        try{
            delegatePs.setShort(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setByte(int parameterIndex,byte x) throws SQLException{
        try{
            delegatePs.setByte(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setBoolean(int parameterIndex,boolean x) throws SQLException{
        try{
            delegatePs.setBoolean(parameterIndex,x);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public void setNull(int parameterIndex,int sqlType) throws SQLException{
        try{
            delegatePs.setNull(parameterIndex,sqlType);
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public int executeUpdate() throws SQLException{
        try{
            return delegatePs.executeUpdate();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException{
        try{
            return delegatePs.executeQuery();
        }catch(SQLException se){
            reportError(se);
            throw se;
        }
    }
}
