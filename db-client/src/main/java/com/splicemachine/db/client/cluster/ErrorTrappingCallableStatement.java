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
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 9/1/16
 */
abstract class ErrorTrappingCallableStatement extends ErrorTrappingPreparedStatement implements CallableStatement{
    private final CallableStatement cs;

    ErrorTrappingCallableStatement(CallableStatement delegate){
        super(delegate);
        this.cs = delegate;
    }

    @Override
    public void registerOutParameter(int parameterIndex,int sqlType) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex,int sqlType,int scale) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean wasNull() throws SQLException{
        try{
            return cs.wasNull();
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getString(int parameterIndex) throws SQLException{
        try{
            return cs.getString(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException{
        try{
            return cs.getBoolean(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException{
        try{
            return cs.getByte(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException{
        try{
            return cs.getShort(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException{
        try{
            return cs.getInt(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException{
        try{
            return cs.getLong(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException{
        try{
            return cs.getFloat(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException{
        try{
            return cs.getDouble(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parameterIndex,int scale) throws SQLException{
        try{
            return cs.getBigDecimal(parameterIndex,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException{
        try{
            return cs.getBytes(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException{
        try{
            return cs.getDate(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException{
        try{
            return cs.getTime(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException{
        try{
            return cs.getTimestamp(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException{
        try{
            return cs.getObject(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException{
        try{
            return cs.getBigDecimal(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Object getObject(int parameterIndex,Map<String, Class<?>> map) throws SQLException{
        try{
            return cs.getObject(parameterIndex,map);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException{
        try{
            return cs.getRef(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException{
        try{
            return cs.getBlob(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException{
        try{
            return cs.getClob(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException{
        try{
            return cs.getArray(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Date getDate(int parameterIndex,Calendar cal) throws SQLException{
        try{
            return cs.getDate(parameterIndex,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Time getTime(int parameterIndex,Calendar cal) throws SQLException{
        try{
            return cs.getTime(parameterIndex,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex,Calendar cal) throws SQLException{
        try{
            return cs.getTimestamp(parameterIndex,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex,int sqlType,String typeName) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType,typeName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType,int scale) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,int sqlType,String typeName) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType,typeName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException{
        try{
            return cs.getURL(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setURL(String parameterName,URL val) throws SQLException{
        try{
            cs.setURL(parameterName,val);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNull(String parameterName,int sqlType) throws SQLException{
        try{
            cs.setNull(parameterName,sqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBoolean(String parameterName,boolean x) throws SQLException{
        try{
            cs.setBoolean(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setByte(String parameterName,byte x) throws SQLException{
        try{
            cs.setByte(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setShort(String parameterName,short x) throws SQLException{
        try{
            cs.setShort(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setInt(String parameterName,int x) throws SQLException{
        try{
            cs.setInt(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setLong(String parameterName,long x) throws SQLException{
        try{
            cs.setLong(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setFloat(String parameterName,float x) throws SQLException{
        try{
            cs.setFloat(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setDouble(String parameterName,double x) throws SQLException{
        try{
            cs.setDouble(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBigDecimal(String parameterName,BigDecimal x) throws SQLException{
        try{
            cs.setBigDecimal(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setString(String parameterName,String x) throws SQLException{
        try{
            cs.setString(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBytes(String parameterName,byte[] x) throws SQLException{
        try{
            cs.setBytes(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setDate(String parameterName,Date x) throws SQLException{
        try{
            cs.setDate(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTime(String parameterName,Time x) throws SQLException{
        try{
            cs.setTime(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTimestamp(String parameterName,Timestamp x) throws SQLException{
        try{
            cs.setTimestamp(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x,int length) throws SQLException{
        try{
            cs.setAsciiStream(parameterName,x,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x,int length) throws SQLException{
        try{
            cs.setBinaryStream(parameterName,x,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setObject(String parameterName,Object x,int targetSqlType,int scale) throws SQLException{
        try{
            cs.setObject(parameterName,x,targetSqlType,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setObject(String parameterName,Object x,int targetSqlType) throws SQLException{
        try{
            cs.setObject(parameterName,x,targetSqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setObject(String parameterName,Object x) throws SQLException{
        try{
            cs.setObject(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader,int length) throws SQLException{
        try{
            cs.setCharacterStream(parameterName,reader,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setDate(String parameterName,Date x,Calendar cal) throws SQLException{
        try{
            cs.setDate(parameterName,x,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTime(String parameterName,Time x,Calendar cal) throws SQLException{
        try{
            cs.setTime(parameterName,x,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setTimestamp(String parameterName,Timestamp x,Calendar cal) throws SQLException{
        try{
            cs.setTimestamp(parameterName,x,cal);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNull(String parameterName,int sqlType,String typeName) throws SQLException{
        try{
            cs.setNull(parameterName,sqlType,typeName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException{
        try{
            return cs.getString(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException{
        try{
            return cs.getBoolean(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public byte getByte(String parameterName) throws SQLException{
        try{
            return cs.getByte(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public short getShort(String parameterName) throws SQLException{
        try{
            return cs.getShort(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public int getInt(String parameterName) throws SQLException{
        try{
            return cs.getInt(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public long getLong(String parameterName) throws SQLException{
        try{
            return cs.getLong(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public float getFloat(String parameterName) throws SQLException{
        try{
            return cs.getFloat(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public double getDouble(String parameterName) throws SQLException{
        try{
            return cs.getDouble(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException{
        try{
            return cs.getBytes(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Date getDate(String parameterName) throws SQLException{
        try{
            return cs.getDate(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Time getTime(String parameterName) throws SQLException{
        try{
            return cs.getTime(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException{
        try{
            return cs.getTimestamp(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Object getObject(String parameterName) throws SQLException{
        try{
            return cs.getObject(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException{
        try{
            return cs.getBigDecimal(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Object getObject(String parameterName,Map<String, Class<?>> map) throws SQLException{
        try{
            return cs.getObject(parameterName,map);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException{
        try{
            return cs.getRef(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException{
        return cs.getBlob(parameterName);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException{
        return cs.getClob(parameterName);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException{
        return cs.getArray(parameterName);
    }

    @Override
    public Date getDate(String parameterName,Calendar cal) throws SQLException{
        return cs.getDate(parameterName,cal);
    }

    @Override
    public Time getTime(String parameterName,Calendar cal) throws SQLException{
        return cs.getTime(parameterName,cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName,Calendar cal) throws SQLException{
        return cs.getTimestamp(parameterName,cal);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException{
        return cs.getURL(parameterName);
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException{
        return cs.getRowId(parameterIndex);
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException{
        return cs.getRowId(parameterName);
    }

    @Override
    public void setRowId(String parameterName,RowId x) throws SQLException{
        cs.setRowId(parameterName,x);
    }

    @Override
    public void setNString(String parameterName,String value) throws SQLException{
        cs.setNString(parameterName,value);
    }

    @Override
    public void setNCharacterStream(String parameterName,Reader value,long length) throws SQLException{
        cs.setNCharacterStream(parameterName,value,length);
    }

    @Override
    public void setNClob(String parameterName,NClob value) throws SQLException{
        try{
            cs.setNClob(parameterName,value);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setClob(String parameterName,Reader reader,long length) throws SQLException{
        try{
            cs.setClob(parameterName,reader,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBlob(String parameterName,InputStream inputStream,long length) throws SQLException{
        try{
            cs.setBlob(parameterName,inputStream,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNClob(String parameterName,Reader reader,long length) throws SQLException{
        try{
            cs.setNClob(parameterName,reader,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException{
        try{
            return cs.getNClob(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException{
        try{
            return cs.getNClob(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setSQLXML(String parameterName,SQLXML xmlObject) throws SQLException{
        try{
            cs.setSQLXML(parameterName,xmlObject);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException{
        try{
            return cs.getSQLXML(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException{
        try{
            return cs.getSQLXML(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException{
        try{
            return cs.getNString(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public String getNString(String parameterName) throws SQLException{
        try{
            return cs.getNString(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException{
        try{
            return cs.getNCharacterStream(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException{
        try{
            return cs.getNCharacterStream(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException{
        try{
            return cs.getCharacterStream(parameterIndex);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException{
        try{
            return cs.getCharacterStream(parameterName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBlob(String parameterName,Blob x) throws SQLException{
        try{
            cs.setBlob(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setClob(String parameterName,Clob x) throws SQLException{
        try{
            cs.setClob(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x,long length) throws SQLException{
        try{
            cs.setAsciiStream(parameterName,x,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x,long length) throws SQLException{
        try{
            cs.setBinaryStream(parameterName,x,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader,long length) throws SQLException{
        try{
            cs.setCharacterStream(parameterName,reader,length);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setAsciiStream(String parameterName,InputStream x) throws SQLException{
        try{
            cs.setAsciiStream(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBinaryStream(String parameterName,InputStream x) throws SQLException{
        try{
            cs.setBinaryStream(parameterName,x);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setCharacterStream(String parameterName,Reader reader) throws SQLException{
        try{
            cs.setCharacterStream(parameterName,reader);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNCharacterStream(String parameterName,Reader value) throws SQLException{
        try{
            cs.setNCharacterStream(parameterName,value);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setClob(String parameterName,Reader reader) throws SQLException{
        try{
            cs.setClob(parameterName,reader);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setBlob(String parameterName,InputStream inputStream) throws SQLException{
        try{
            cs.setBlob(parameterName,inputStream);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setNClob(String parameterName,Reader reader) throws SQLException{
        try{
            cs.setNClob(parameterName,reader);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public <T> T getObject(int parameterIndex,Class<T> type) throws SQLException{
        try{
            return cs.getObject(parameterIndex,type);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public <T> T getObject(String parameterName,Class<T> type) throws SQLException{
        try{
            return cs.getObject(parameterName,type);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setObject(String parameterName,Object x,SQLType targetSqlType,int scaleOrLength) throws SQLException{
        try{
            cs.setObject(parameterName,x,targetSqlType,scaleOrLength);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void setObject(String parameterName,Object x,SQLType targetSqlType) throws SQLException{
        try{
            cs.setObject(parameterName,x,targetSqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex,SQLType sqlType) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex,SQLType sqlType,int scale) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex,SQLType sqlType,String typeName) throws SQLException{
        try{
            cs.registerOutParameter(parameterIndex,sqlType,typeName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,SQLType sqlType) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,SQLType sqlType,int scale) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType,scale);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }

    @Override
    public void registerOutParameter(String parameterName,SQLType sqlType,String typeName) throws SQLException{
        try{
            cs.registerOutParameter(parameterName,sqlType,typeName);
        }catch(SQLException e){
            reportError(e);
            throw e;
        }
    }
}
