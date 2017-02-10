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

package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.ManagedPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 8/4/14
 */
public class ManagedCallableStatement extends ManagedPreparedStatement implements CallableStatement {
    private final CallableStatement delegate;

    public ManagedCallableStatement(CallableStatement delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException { delegate.registerOutParameter(parameterIndex, sqlType); }
    @Override public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException { delegate.registerOutParameter(parameterIndex, sqlType, scale); }
    @Override public boolean wasNull() throws SQLException { return delegate.wasNull(); }
    @Override public String getString(int parameterIndex) throws SQLException { return delegate.getString(parameterIndex); }
    @Override public boolean getBoolean(int parameterIndex) throws SQLException { return delegate.getBoolean(parameterIndex); }
    @Override public byte getByte(int parameterIndex) throws SQLException { return delegate.getByte(parameterIndex); }
    @Override public short getShort(int parameterIndex) throws SQLException { return delegate.getShort(parameterIndex); }
    @Override public int getInt(int parameterIndex) throws SQLException { return delegate.getInt(parameterIndex); }
    @Override public long getLong(int parameterIndex) throws SQLException { return delegate.getLong(parameterIndex); }
    @Override public float getFloat(int parameterIndex) throws SQLException { return delegate.getFloat(parameterIndex); }
    @Override public double getDouble(int parameterIndex) throws SQLException { return delegate.getDouble(parameterIndex); }
    @Override public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException { return delegate.getBigDecimal(parameterIndex, scale); }
    @Override public byte[] getBytes(int parameterIndex) throws SQLException { return delegate.getBytes(parameterIndex); }
    @Override public Date getDate(int parameterIndex) throws SQLException { return delegate.getDate(parameterIndex); }
    @Override public Time getTime(int parameterIndex) throws SQLException { return delegate.getTime(parameterIndex); }
    @Override public Timestamp getTimestamp(int parameterIndex) throws SQLException { return delegate.getTimestamp(parameterIndex); }
    @Override public Object getObject(int parameterIndex) throws SQLException { return delegate.getObject(parameterIndex); }
    @Override public BigDecimal getBigDecimal(int parameterIndex) throws SQLException { return delegate.getBigDecimal(parameterIndex); }
    @Override public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException { return delegate.getObject(parameterIndex, map); }
    @Override public Ref getRef(int parameterIndex) throws SQLException { return delegate.getRef(parameterIndex); }
    @Override public Blob getBlob(int parameterIndex) throws SQLException { return delegate.getBlob(parameterIndex); }
    @Override public Clob getClob(int parameterIndex) throws SQLException { return delegate.getClob(parameterIndex); }
    @Override public Array getArray(int parameterIndex) throws SQLException { return delegate.getArray(parameterIndex); }
    @Override public Date getDate(int parameterIndex, Calendar cal) throws SQLException { return delegate.getDate(parameterIndex, cal); }
    @Override public Time getTime(int parameterIndex, Calendar cal) throws SQLException { return delegate.getTime(parameterIndex, cal); }
    @Override public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException { return delegate.getTimestamp(parameterIndex, cal); }
    @Override public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException { delegate.registerOutParameter(parameterIndex, sqlType, typeName); }
    @Override public void registerOutParameter(String parameterName, int sqlType) throws SQLException { delegate.registerOutParameter(parameterName, sqlType); }
    @Override public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException { delegate.registerOutParameter(parameterName, sqlType, scale); }
    @Override public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException { delegate.registerOutParameter(parameterName, sqlType, typeName); }
    @Override public URL getURL(int parameterIndex) throws SQLException { return delegate.getURL(parameterIndex); }
    @Override public void setURL(String parameterName, URL val) throws SQLException { delegate.setURL(parameterName, val); }
    @Override public void setNull(String parameterName, int sqlType) throws SQLException { delegate.setNull(parameterName, sqlType); }
    @Override public void setBoolean(String parameterName, boolean x) throws SQLException { delegate.setBoolean(parameterName, x); }
    @Override public void setByte(String parameterName, byte x) throws SQLException { delegate.setByte(parameterName, x); }
    @Override public void setShort(String parameterName, short x) throws SQLException { delegate.setShort(parameterName, x); }
    @Override public void setInt(String parameterName, int x) throws SQLException { delegate.setInt(parameterName, x); }
    @Override public void setLong(String parameterName, long x) throws SQLException { delegate.setLong(parameterName, x); }
    @Override public void setFloat(String parameterName, float x) throws SQLException { delegate.setFloat(parameterName, x); }
    @Override public void setDouble(String parameterName, double x) throws SQLException { delegate.setDouble(parameterName, x); }
    @Override public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException { delegate.setBigDecimal(parameterName, x); }
    @Override public void setString(String parameterName, String x) throws SQLException { delegate.setString(parameterName, x); }
    @Override public void setBytes(String parameterName, byte[] x) throws SQLException { delegate.setBytes(parameterName, x); }
    @Override public void setDate(String parameterName, Date x) throws SQLException { delegate.setDate(parameterName, x); }
    @Override public void setTime(String parameterName, Time x) throws SQLException { delegate.setTime(parameterName, x); }
    @Override public void setTimestamp(String parameterName, Timestamp x) throws SQLException { delegate.setTimestamp(parameterName, x); }
    @Override public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException { delegate.setAsciiStream(parameterName, x, length); }
    @Override public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException { delegate.setBinaryStream(parameterName, x, length); }
    @Override public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException { delegate.setObject(parameterName, x, targetSqlType, scale); }
    @Override public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException { delegate.setObject(parameterName, x, targetSqlType); }
    @Override public void setObject(String parameterName, Object x) throws SQLException { delegate.setObject(parameterName, x); }
    @Override public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException { delegate.setCharacterStream(parameterName, reader, length); }
    @Override public void setDate(String parameterName, Date x, Calendar cal) throws SQLException { delegate.setDate(parameterName, x, cal); }
    @Override public void setTime(String parameterName, Time x, Calendar cal) throws SQLException { delegate.setTime(parameterName, x, cal); }
    @Override public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException { delegate.setTimestamp(parameterName, x, cal); }
    @Override public void setNull(String parameterName, int sqlType, String typeName) throws SQLException { delegate.setNull(parameterName, sqlType, typeName); }
    @Override public String getString(String parameterName) throws SQLException { return delegate.getString(parameterName); }
    @Override public boolean getBoolean(String parameterName) throws SQLException { return delegate.getBoolean(parameterName); }
    @Override public byte getByte(String parameterName) throws SQLException { return delegate.getByte(parameterName); }
    @Override public short getShort(String parameterName) throws SQLException { return delegate.getShort(parameterName); }
    @Override public int getInt(String parameterName) throws SQLException { return delegate.getInt(parameterName); }
    @Override public long getLong(String parameterName) throws SQLException { return delegate.getLong(parameterName); }
    @Override public float getFloat(String parameterName) throws SQLException { return delegate.getFloat(parameterName); }
    @Override public double getDouble(String parameterName) throws SQLException { return delegate.getDouble(parameterName); }
    @Override public byte[] getBytes(String parameterName) throws SQLException { return delegate.getBytes(parameterName); }
    @Override public Date getDate(String parameterName) throws SQLException { return delegate.getDate(parameterName); }
    @Override public Time getTime(String parameterName) throws SQLException { return delegate.getTime(parameterName); }
    @Override public Timestamp getTimestamp(String parameterName) throws SQLException { return delegate.getTimestamp(parameterName); }
    @Override public Object getObject(String parameterName) throws SQLException { return delegate.getObject(parameterName); }
    @Override public BigDecimal getBigDecimal(String parameterName) throws SQLException { return delegate.getBigDecimal(parameterName); }
    @Override public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException { return delegate.getObject(parameterName, map); }
    @Override public Ref getRef(String parameterName) throws SQLException { return delegate.getRef(parameterName); }
    @Override public Blob getBlob(String parameterName) throws SQLException { return delegate.getBlob(parameterName); }
    @Override public Clob getClob(String parameterName) throws SQLException { return delegate.getClob(parameterName); }
    @Override public Array getArray(String parameterName) throws SQLException { return delegate.getArray(parameterName); }
    @Override public Date getDate(String parameterName, Calendar cal) throws SQLException { return delegate.getDate(parameterName, cal); }
    @Override public Time getTime(String parameterName, Calendar cal) throws SQLException { return delegate.getTime(parameterName, cal); }
    @Override public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException { return delegate.getTimestamp(parameterName, cal); }
    @Override public URL getURL(String parameterName) throws SQLException { return delegate.getURL(parameterName); }
    @Override public RowId getRowId(int parameterIndex) throws SQLException { return delegate.getRowId(parameterIndex); }
    @Override public RowId getRowId(String parameterName) throws SQLException { return delegate.getRowId(parameterName); }
    @Override public void setRowId(String parameterName, RowId x) throws SQLException { delegate.setRowId(parameterName, x); }
    @Override public void setNString(String parameterName, String value) throws SQLException { delegate.setNString(parameterName, value); }
    @Override public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException { delegate.setNCharacterStream(parameterName, value, length); }
    @Override public void setNClob(String parameterName, NClob value) throws SQLException { delegate.setNClob(parameterName, value); }
    @Override public void setClob(String parameterName, Reader reader, long length) throws SQLException { delegate.setClob(parameterName, reader, length); }
    @Override public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException { delegate.setBlob(parameterName, inputStream, length); }
    @Override public void setNClob(String parameterName, Reader reader, long length) throws SQLException { delegate.setNClob(parameterName, reader, length); }
    @Override public NClob getNClob(int parameterIndex) throws SQLException { return delegate.getNClob(parameterIndex); }
    @Override public NClob getNClob(String parameterName) throws SQLException { return delegate.getNClob(parameterName); }
    @Override public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException { delegate.setSQLXML(parameterName, xmlObject); }
    @Override public SQLXML getSQLXML(int parameterIndex) throws SQLException { return delegate.getSQLXML(parameterIndex); }
    @Override public SQLXML getSQLXML(String parameterName) throws SQLException { return delegate.getSQLXML(parameterName); }
    @Override public String getNString(int parameterIndex) throws SQLException { return delegate.getNString(parameterIndex); }
    @Override public String getNString(String parameterName) throws SQLException { return delegate.getNString(parameterName); }
    @Override public Reader getNCharacterStream(int parameterIndex) throws SQLException { return delegate.getNCharacterStream(parameterIndex); }
    @Override public Reader getNCharacterStream(String parameterName) throws SQLException { return delegate.getNCharacterStream(parameterName); }
    @Override public Reader getCharacterStream(int parameterIndex) throws SQLException { return delegate.getCharacterStream(parameterIndex); }
    @Override public Reader getCharacterStream(String parameterName) throws SQLException { return delegate.getCharacterStream(parameterName); }
    @Override public void setBlob(String parameterName, Blob x) throws SQLException { delegate.setBlob(parameterName, x); }
    @Override public void setClob(String parameterName, Clob x) throws SQLException { delegate.setClob(parameterName, x); }
    @Override public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException { delegate.setAsciiStream(parameterName, x, length); }
    @Override public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException { delegate.setBinaryStream(parameterName, x, length); }
    @Override public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException { delegate.setCharacterStream(parameterName, reader, length); }
    @Override public void setAsciiStream(String parameterName, InputStream x) throws SQLException { delegate.setAsciiStream(parameterName, x); }
    @Override public void setBinaryStream(String parameterName, InputStream x) throws SQLException { delegate.setBinaryStream(parameterName, x); }
    @Override public void setCharacterStream(String parameterName, Reader reader) throws SQLException { delegate.setCharacterStream(parameterName, reader); }
    @Override public void setNCharacterStream(String parameterName, Reader value) throws SQLException { delegate.setNCharacterStream(parameterName, value); }
    @Override public void setClob(String parameterName, Reader reader) throws SQLException { delegate.setClob(parameterName, reader); }
    @Override public void setBlob(String parameterName, InputStream inputStream) throws SQLException { delegate.setBlob(parameterName, inputStream); }
    @Override public void setNClob(String parameterName, Reader reader) throws SQLException { delegate.setNClob(parameterName, reader); }
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return type.cast(delegate.getObject(parameterIndex));
    }
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return type.cast(delegate.getObject(parameterName));
    }
}
