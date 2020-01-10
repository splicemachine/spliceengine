/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.sql.*;
import java.math.BigDecimal;

import java.util.Calendar;
import java.util.Map;


/**
	JDBC 2 brokered CallableStatement
 */
public abstract class BrokeredCallableStatement extends BrokeredPreparedStatement
          implements CallableStatement
{

	public BrokeredCallableStatement(BrokeredStatementControl control, String sql) throws SQLException {
		super(control,sql);
	}

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType);
    }

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType,
                                     int scale)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType, scale);
    }

    public final boolean wasNull()
        throws SQLException
    {
        return getCallableStatement().wasNull();
    }

    public final void close() throws SQLException {
        control.closeRealCallableStatement();
    }
    
    public final String getString(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getString( parameterIndex);
    }

    public final boolean getBoolean(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBoolean( parameterIndex);
    }

    public final byte getByte(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getByte( parameterIndex);
    }

    public final short getShort(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getShort( parameterIndex);
    }

    public final int getInt(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getInt( parameterIndex);
    }

    public final long getLong(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getLong( parameterIndex);
    }

    public final float getFloat(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getFloat( parameterIndex);
    }

    public final double getDouble(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDouble( parameterIndex);
    }

    /** @deprecated */
    public final BigDecimal getBigDecimal(int parameterIndex,
                                              int scale)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex, scale);
    }

    public final byte[] getBytes(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBytes( parameterIndex);
    }

    public final Date getDate(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex);
    }

    public final Date getDate(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex, cal);
    }

    public final Time getTime(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex);
    }

    public final Timestamp getTimestamp(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex);
    }

    public final Object getObject(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getObject( parameterIndex);
    }

    public final BigDecimal getBigDecimal(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex);
    }

    public final Object getObject(int i,
                            Map map)
        throws SQLException
    {
        return getCallableStatement().getObject( i, map);
    }

    public final Ref getRef(int i)
        throws SQLException
    {
        return getCallableStatement().getRef( i);
    }

    public final Blob getBlob(int i)
        throws SQLException
    {
        return getCallableStatement().getBlob( i);
    }

    public final Clob getClob(int i)
        throws SQLException
    {
        return getCallableStatement().getClob( i);
    }

    public final Array getArray(int i)
        throws SQLException
    {
        return getCallableStatement().getArray( i);
    }

    public final Time getTime(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex, cal);
    }

    public final Timestamp getTimestamp(int parameterIndex,
                                  Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex, cal);
    }

    public final void registerOutParameter(int paramIndex,
                                     int sqlType,
                                     String typeName)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( paramIndex, sqlType, typeName);
    }

	/*
	** Control methods
	*/

    /**
     * Access the underlying CallableStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    final CallableStatement getCallableStatement() throws SQLException {
		return control.getRealCallableStatement();
	}
	
    /**
     * Access the underlying PreparedStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    final PreparedStatement getPreparedStatement() throws SQLException {
		return getCallableStatement();
	}
	/**
		Create a duplicate CalableStatement to this, including state, from the passed in Connection.
	*/
	public CallableStatement createDuplicateStatement(Connection conn, CallableStatement oldStatement) throws SQLException {

		CallableStatement newStatement = conn.prepareCall(sql, resultSetType, resultSetConcurrency);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}

    //@Override
    public void registerOutParameter(String parameterName,int sqlType) throws SQLException{
        getCallableStatement().registerOutParameter(parameterName, sqlType);
    }

    //@Override
    public void registerOutParameter(String parameterName,int sqlType,int scale) throws SQLException{
        getCallableStatement().registerOutParameter(parameterName, sqlType,scale);
    }

    //@Override
    public void registerOutParameter(String parameterName,int sqlType,String typeName) throws SQLException{
        getCallableStatement().registerOutParameter(parameterName, sqlType,typeName);
    }

    //@Override
    public URL getURL(int parameterIndex) throws SQLException{
        return getCallableStatement().getURL(parameterIndex);
    }

    //@Override
    public void setURL(String parameterName,URL val) throws SQLException{
        getCallableStatement().setURL(parameterName,val);
    }

    //@Override
    public void setNull(String parameterName,int sqlType) throws SQLException{
        getCallableStatement().setNull(parameterName, sqlType);
    }

    //@Override
    public void setBoolean(String parameterName,boolean x) throws SQLException{
        getCallableStatement().setBoolean(parameterName, x);
    }

    //@Override
    public void setByte(String parameterName,byte x) throws SQLException{
        getCallableStatement().setByte(parameterName, x);
    }

    //@Override
    public void setShort(String parameterName,short x) throws SQLException{
        getCallableStatement().setShort(parameterName, x);
    }

    //@Override
    public void setInt(String parameterName,int x) throws SQLException{
        getCallableStatement().setInt(parameterName,x);
    }

    //@Override
    public void setLong(String parameterName,long x) throws SQLException{
        getCallableStatement().setLong(parameterName,x);
    }

    //@Override
    public void setFloat(String parameterName,float x) throws SQLException{
        getCallableStatement().setFloat(parameterName,x);
    }

    //@Override
    public void setDouble(String parameterName,double x) throws SQLException{
        getCallableStatement().setDouble(parameterName,x);
    }

    //@Override
    public void setBigDecimal(String parameterName,BigDecimal x) throws SQLException{
        getCallableStatement().setBigDecimal(parameterName,x);
    }

    //@Override
    public void setString(String parameterName,String x) throws SQLException{
        getCallableStatement().setString(parameterName,x);
    }

    //@Override
    public void setBytes(String parameterName,byte[] x) throws SQLException{
        getCallableStatement().setBytes(parameterName,x);
    }

    //@Override
    public void setDate(String parameterName,Date x) throws SQLException{
        getCallableStatement().setDate(parameterName,x);
    }

    //@Override
    public void setTime(String parameterName,Time x) throws SQLException{
        getCallableStatement().setTime(parameterName,x);
    }

    //@Override
    public void setTimestamp(String parameterName,Timestamp x) throws SQLException{
        getCallableStatement().setTimestamp(parameterName,x);
    }

    //@Override
    public void setAsciiStream(String parameterName,InputStream x,int length) throws SQLException{
        getCallableStatement().setAsciiStream(parameterName,x,length);
    }

    //@Override
    public void setBinaryStream(String parameterName,InputStream x,int length) throws SQLException{
        getCallableStatement().setBinaryStream(parameterName,x,length);
    }

    //@Override
    public void setObject(String parameterName,Object x,int targetSqlType,int scale) throws SQLException{
        getCallableStatement().setObject(parameterName,x,targetSqlType,scale);
    }

    //@Override
    public void setObject(String parameterName,Object x,int targetSqlType) throws SQLException{
        getCallableStatement().setObject(parameterName,x,targetSqlType);
    }

    //@Override
    public void setObject(String parameterName,Object x) throws SQLException{
        getCallableStatement().setObject(parameterName,x);
    }

    //@Override
    public void setCharacterStream(String parameterName,Reader reader,int length) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName,reader,length);
    }

    //@Override
    public void setDate(String parameterName,Date x,Calendar cal) throws SQLException{
        getCallableStatement().setDate(parameterName,x,cal);
    }

    //@Override
    public void setTime(String parameterName,Time x,Calendar cal) throws SQLException{
        getCallableStatement().setTime(parameterName,x,cal);
    }

    //@Override
    public void setTimestamp(String parameterName,Timestamp x,Calendar cal) throws SQLException{
        getCallableStatement().setTimestamp(parameterName,x,cal);
    }

    //@Override
    public void setNull(String parameterName,int sqlType,String typeName) throws SQLException{
        getCallableStatement().setNull(parameterName,sqlType,typeName);
    }

    //@Override
    public String getString(String parameterName) throws SQLException{
        return getCallableStatement().getString(parameterName);
    }

    //@Override
    public boolean getBoolean(String parameterName) throws SQLException{
        return getCallableStatement().getBoolean(parameterName);
    }

    //@Override
    public byte getByte(String parameterName) throws SQLException{
        return getCallableStatement().getByte(parameterName);
    }

    //@Override
    public short getShort(String parameterName) throws SQLException{
        return getCallableStatement().getShort(parameterName);
    }

    //@Override
    public int getInt(String parameterName) throws SQLException{
        return getCallableStatement().getInt(parameterName);
    }

    //@Override
    public long getLong(String parameterName) throws SQLException{
        return getCallableStatement().getLong(parameterName);
    }

    //@Override
    public float getFloat(String parameterName) throws SQLException{
        return getCallableStatement().getFloat(parameterName);
    }

    //@Override
    public double getDouble(String parameterName) throws SQLException{
        return getCallableStatement().getDouble(parameterName);
    }

    //@Override
    public byte[] getBytes(String parameterName) throws SQLException{
        return getCallableStatement().getBytes(parameterName);
    }

    //@Override
    public Date getDate(String parameterName) throws SQLException{
        return getCallableStatement().getDate(parameterName);
    }

    //@Override
    public Time getTime(String parameterName) throws SQLException{
        return getCallableStatement().getTime(parameterName);
    }

    //@Override
    public Timestamp getTimestamp(String parameterName) throws SQLException{
        return getCallableStatement().getTimestamp(parameterName);
    }

    //@Override
    public Object getObject(String parameterName) throws SQLException{
        return getCallableStatement().getObject(parameterName);
    }

    //@Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException{
        return getCallableStatement().getBigDecimal(parameterName);
    }

    //@Override
    public Object getObject(String parameterName,Map<String, Class<?>> map) throws SQLException{
        return getCallableStatement().getObject(parameterName,map);
    }

    //@Override
    public Ref getRef(String parameterName) throws SQLException{
        return getCallableStatement().getRef(parameterName);
    }

    //@Override
    public Blob getBlob(String parameterName) throws SQLException{
        return getCallableStatement().getBlob(parameterName);
    }

    //@Override
    public Clob getClob(String parameterName) throws SQLException{
        return getCallableStatement().getClob(parameterName);
    }

    //@Override
    public Array getArray(String parameterName) throws SQLException{
        return getCallableStatement().getArray(parameterName);
    }

    //@Override
    public Date getDate(String parameterName,Calendar cal) throws SQLException{
        return getCallableStatement().getDate(parameterName,cal);
    }

    //@Override
    public Time getTime(String parameterName,Calendar cal) throws SQLException{
        return getCallableStatement().getTime(parameterName,cal);
    }

    //@Override
    public Timestamp getTimestamp(String parameterName,Calendar cal) throws SQLException{
        return getCallableStatement().getTimestamp(parameterName,cal);
    }

    //@Override
    public URL getURL(String parameterName) throws SQLException{
        return getCallableStatement().getURL(parameterName);
    }

    //@Override
    public RowId getRowId(int parameterIndex) throws SQLException{
        return getCallableStatement().getRowId(parameterIndex);
    }

    //@Override
    public RowId getRowId(String parameterName) throws SQLException{
        return getCallableStatement().getRowId(parameterName);
    }

    //@Override
    public void setRowId(String parameterName,RowId x) throws SQLException{
        getCallableStatement().setRowId(parameterName, x);
    }

    //@Override
    public void setNString(String parameterName,String value) throws SQLException{
        getCallableStatement().setNString(parameterName, value);
    }

    //@Override
    public void setNCharacterStream(String parameterName,Reader value,long length) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName, value, length);
    }

    //@Override
    public void setNClob(String parameterName,NClob value) throws SQLException{
        getCallableStatement().setNClob(parameterName,value);
    }

    //@Override
    public void setClob(String parameterName,Reader reader,long length) throws SQLException{
        getCallableStatement().setClob(parameterName,reader,length);
    }

    //@Override
    public void setBlob(String parameterName,InputStream inputStream,long length) throws SQLException{
        getCallableStatement().setBlob(parameterName,inputStream,length);
    }

    //@Override
    public void setNClob(String parameterName,Reader reader,long length) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName, reader, length);
    }

    //@Override
    public NClob getNClob(int parameterIndex) throws SQLException{
        return getCallableStatement().getNClob(parameterIndex);
    }

    //@Override
    public NClob getNClob(String parameterName) throws SQLException{
        return getCallableStatement().getNClob(parameterName);
    }

    //@Override
    public void setSQLXML(String parameterName,SQLXML xmlObject) throws SQLException{
        getCallableStatement().setSQLXML(parameterName, xmlObject);
    }

    //@Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException{
        return getCallableStatement().getSQLXML(parameterIndex);
    }

    //@Override
    public SQLXML getSQLXML(String parameterName) throws SQLException{
        return getCallableStatement().getSQLXML(parameterName);
    }

    //@Override
    public String getNString(int parameterIndex) throws SQLException{
        return getCallableStatement().getNString(parameterIndex);
    }

    //@Override
    public String getNString(String parameterName) throws SQLException{
        return getCallableStatement().getNString(parameterName);
    }

    //@Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException{
        return getCallableStatement().getNCharacterStream(parameterIndex);
    }

    //@Override
    public Reader getNCharacterStream(String parameterName) throws SQLException{
        return getCallableStatement().getNCharacterStream(parameterName);
    }

    //@Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException{
        return getCallableStatement().getCharacterStream(parameterIndex);
    }

    //@Override
    public Reader getCharacterStream(String parameterName) throws SQLException{
        return getCallableStatement().getCharacterStream(parameterName);
    }

    //@Override
    public void setBlob(String parameterName,Blob x) throws SQLException{
        getCallableStatement().setBlob(parameterName, x);
    }

    //@Override
    public void setClob(String parameterName,Clob x) throws SQLException{
        getCallableStatement().setClob(parameterName, x);
    }

    //@Override
    public void setAsciiStream(String parameterName,InputStream x,long length) throws SQLException{
        getCallableStatement().setAsciiStream(parameterName, x, length);
    }

    //@Override
    public void setBinaryStream(String parameterName,InputStream x,long length) throws SQLException{
        getCallableStatement().setBinaryStream(parameterName, x, length);
    }

    //@Override
    public void setCharacterStream(String parameterName,Reader reader,long length) throws SQLException{
        getCallableStatement().setCharacterStream(parameterName, reader, length);
    }

    //@Override
    public void setAsciiStream(String parameterName,InputStream x) throws SQLException{
        getCallableStatement().setAsciiStream(parameterName, x);
    }

    //@Override
    public void setBinaryStream(String parameterName,InputStream x) throws SQLException{
        getCallableStatement().setBinaryStream(parameterName, x);
    }

    //@Override
    public void setCharacterStream(String parameterName,Reader reader) throws SQLException{
        getCallableStatement().setCharacterStream(parameterName, reader);
    }

    //@Override
    public void setNCharacterStream(String parameterName,Reader value) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterName, value);
    }

    //@Override
    public void setClob(String parameterName,Reader reader) throws SQLException{
        getCallableStatement().setClob(parameterName, reader);
    }

    //@Override
    public void setBlob(String parameterName,InputStream inputStream) throws SQLException{
        getCallableStatement().setBlob(parameterName, inputStream);
    }

    //@Override
    public void setNClob(String parameterName,Reader reader) throws SQLException{
        getCallableStatement().setNClob(parameterName, reader);
    }

    //@Override
    public <T> T getObject(int parameterIndex,Class<T> type) throws SQLException{
        return getCallableStatement().getObject(parameterIndex, type);
    }

    //@Override
    public <T> T getObject(String parameterName,Class<T> type) throws SQLException{
        return getCallableStatement().getObject(parameterName, type);
    }

    //@Override
    public void setURL(int parameterIndex,URL x) throws SQLException{
        getCallableStatement().setURL(parameterIndex, x);
    }

    //@Override
    public ParameterMetaData getParameterMetaData() throws SQLException{
        return getCallableStatement().getParameterMetaData();
    }

    //@Override
    public void setRowId(int parameterIndex,RowId x) throws SQLException{
        getCallableStatement().setRowId(parameterIndex,x);
    }

    //@Override
    public void setNString(int parameterIndex,String value) throws SQLException{
        getCallableStatement().setNString(parameterIndex, value);
    }

    //@Override
    public void setNCharacterStream(int parameterIndex,Reader value,long length) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterIndex, value, length);
    }

    //@Override
    public void setNClob(int parameterIndex,NClob value) throws SQLException{
        getCallableStatement().setNClob(parameterIndex,value);
    }

    //@Override
    public void setClob(int parameterIndex,Reader reader,long length) throws SQLException{
        getCallableStatement().setClob(parameterIndex, reader, length);
    }

    //@Override
    public void setBlob(int parameterIndex,InputStream inputStream,long length) throws SQLException{
        getCallableStatement().setBlob(parameterIndex, inputStream, length);
    }

    //@Override
    public void setNClob(int parameterIndex,Reader reader,long length) throws SQLException{
        getCallableStatement().setNClob(parameterIndex,reader,length);
    }

    //@Override
    public void setSQLXML(int parameterIndex,SQLXML xmlObject) throws SQLException{
        getCallableStatement().setSQLXML(parameterIndex, xmlObject);
    }

    //@Override
    public void setAsciiStream(int parameterIndex,InputStream x,long length) throws SQLException{
        getCallableStatement().setAsciiStream(parameterIndex, x, length);
    }

    //@Override
    public void setBinaryStream(int parameterIndex,InputStream x,long length) throws SQLException{
        getCallableStatement().setBinaryStream(parameterIndex, x, length);
    }

    //@Override
    public void setCharacterStream(int parameterIndex,Reader reader,long length) throws SQLException{
        getCallableStatement().setCharacterStream(parameterIndex,reader,length);
    }

    //@Override
    public void setAsciiStream(int parameterIndex,InputStream x) throws SQLException{
        getCallableStatement().setAsciiStream(parameterIndex,x);
    }

    //@Override
    public void setNCharacterStream(int parameterIndex,Reader value) throws SQLException{
        getCallableStatement().setNCharacterStream(parameterIndex, value);
    }

    //@Override
    public void setClob(int parameterIndex,Reader reader) throws SQLException{
        getCallableStatement().setClob(parameterIndex,reader);
    }

    //@Override
    public void setBlob(int parameterIndex,InputStream inputStream) throws SQLException{
        getCallableStatement().setBlob(parameterIndex,inputStream);
    }

    //@Override
    public void setNClob(int parameterIndex,Reader reader) throws SQLException{
        getCallableStatement().setNClob(parameterIndex,reader);
    }
}
