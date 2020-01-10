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

package com.splicemachine.db.vti;

import java.io.InputStream;

import java.io.Reader;
import java.sql.*;
import java.math.BigDecimal;

import java.net.URL;
import java.util.Calendar;

/**
	An  implementation of the JDBC 3.0 ResultSet that is useful
	when writing table functions, read-only VTIs (virtual table interface), and
	the ResultSets returned by executeQuery in read-write VTI classes. This
    implementation raises "unimplemented method" exceptions for all methods.
*/
class VTITemplateBase implements ResultSet
{

    //
    // java.sql.ResultSet calls, passed through to our result set.
    //

    public ResultSetMetaData getMetaData() throws SQLException { throw notImplemented( "getMetaData" ); }
    public boolean next() throws SQLException { throw notImplemented( "next" ); }
    public void close() throws SQLException { throw notImplemented( "close" ); }
    public boolean wasNull() throws SQLException { throw notImplemented( "wasNull" ); }
    public String getString(int columnIndex) throws SQLException { throw notImplemented( "getString" ); }
    public boolean getBoolean(int columnIndex) throws SQLException { throw notImplemented( "getBoolean" ); }
    public byte getByte(int columnIndex) throws SQLException { throw notImplemented( "getByte" ); }
    public short getShort(int columnIndex) throws SQLException { throw notImplemented( "getShort" ); }
    public int getInt(int columnIndex) throws SQLException { throw notImplemented( "getInt" ); }
    public long getLong(int columnIndex) throws SQLException { throw notImplemented( "getLong" ); }
    public float getFloat(int columnIndex) throws SQLException { throw notImplemented( "getFloat" ); }
    public double getDouble(int columnIndex) throws SQLException { throw notImplemented( "getDouble" ); }
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public byte[] getBytes(int columnIndex) throws SQLException { throw notImplemented( "] getBytes" ); }
    public java.sql.Date getDate(int columnIndex) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
    public java.sql.Time getTime(int columnIndex) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getAsciiStream" ); }
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getUnicodeStream" ); }
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException { throw notImplemented( "io.InputStream getBinaryStream" ); }
    public String getString(String columnName) throws SQLException { throw notImplemented( "getString" ); }
    public boolean getBoolean(String columnName) throws SQLException { throw notImplemented( "getBoolean" ); }
    public byte getByte(String columnName) throws SQLException { throw notImplemented( "getByte" ); }
    public short getShort(String columnName) throws SQLException { throw notImplemented( "getShort" ); }
    public int getInt(String columnName) throws SQLException { throw notImplemented( "getInt" ); }
    public long getLong(String columnName) throws SQLException { throw notImplemented( "getLong" ); }
    public float getFloat(String columnName) throws SQLException { throw notImplemented( "getFloat" ); }
    public double getDouble(String columnName) throws SQLException { throw notImplemented( "getDouble" ); }
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public byte[] getBytes(String columnName) throws SQLException { throw notImplemented( "] getBytes" ); }
    public java.sql.Date getDate(String columnName) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
    public java.sql.Time getTime(String columnName) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getAsciiStream" ); }
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getUnicodeStream" ); }
    public java.io.InputStream getBinaryStream(String columnName) throws SQLException { throw notImplemented( "io.InputStream getBinaryStream" ); }
    public SQLWarning getWarnings() throws SQLException { throw notImplemented( "getWarnings" ); }
    public void clearWarnings() throws SQLException { throw notImplemented( "clearWarnings" ); }
    public String getCursorName() throws SQLException { throw notImplemented( "getCursorName" ); }
    public Object getObject(int columnIndex) throws SQLException { throw notImplemented( "getObject" ); }
    public Object getObject(String columnName) throws SQLException { throw notImplemented( "getObject" ); }
    public int findColumn(String columnName) throws SQLException { throw notImplemented( "findColumn" ); }
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException { throw notImplemented( "io.Reader getCharacterStream" ); }
    public java.io.Reader getCharacterStream(String columnName) throws SQLException { throw notImplemented( "io.Reader getCharacterStream" ); }
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public BigDecimal getBigDecimal(String columnName) throws SQLException { throw notImplemented( "getBigDecimal" ); }
    public boolean isBeforeFirst() throws SQLException { throw notImplemented( "isBeforeFirst" ); }
    public boolean isAfterLast() throws SQLException { throw notImplemented( "isAfterLast" ); }
    public boolean isFirst() throws SQLException { throw notImplemented( "isFirst" ); }
    public boolean isLast() throws SQLException { throw notImplemented( "isLast" ); }
    public void beforeFirst() throws SQLException { throw notImplemented( "beforeFirst" ); }
    public void afterLast() throws SQLException { throw notImplemented( "afterLast" ); }
    public boolean first() throws SQLException { throw notImplemented( "first" ); }
    public boolean last() throws SQLException { throw notImplemented( "last" ); }
    public int getRow() throws SQLException { throw notImplemented( "getRow" ); }
    public boolean absolute(int row) throws SQLException { throw notImplemented( "absolute" ); }
    public boolean relative(int rows) throws SQLException { throw notImplemented( "relative" ); }
    public boolean previous() throws SQLException { throw notImplemented( "previous" ); }
    public void setFetchDirection(int direction) throws SQLException { throw notImplemented( "setFetchDirection" ); }
    public int getFetchDirection() throws SQLException { throw notImplemented( "getFetchDirection" ); }
    public void setFetchSize(int rows) throws SQLException { throw notImplemented( "setFetchSize" ); }
    public int getFetchSize() throws SQLException { throw notImplemented( "getFetchSize" ); }
    public int getType() throws SQLException { throw notImplemented( "getType" ); }
    public int getConcurrency() throws SQLException { throw notImplemented( "getConcurrency" ); }
    public boolean rowUpdated() throws SQLException { throw notImplemented( "rowUpdated" ); }
    public boolean rowInserted() throws SQLException { throw notImplemented( "rowInserted" ); }
    public boolean rowDeleted() throws SQLException { throw notImplemented( "rowDeleted" ); }
    public void updateNull(int columnIndex) throws SQLException { throw notImplemented( "updateNull" ); }
    public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw notImplemented( "updateBoolean" ); }
    public void updateByte(int columnIndex, byte x) throws SQLException { throw notImplemented( "updateByte" ); }
    public void updateShort(int columnIndex, short x) throws SQLException { throw notImplemented( "updateShort" ); }
    public void updateInt(int columnIndex, int x) throws SQLException { throw notImplemented( "updateInt" ); }
    public void updateLong(int columnIndex, long x) throws SQLException { throw notImplemented( "updateLong" ); }
    public void updateFloat(int columnIndex, float x) throws SQLException { throw notImplemented( "updateFloat" ); }
    public void updateDouble(int columnIndex, double x) throws SQLException { throw notImplemented( "updateDouble" ); }
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw notImplemented( "updateBigDecimal" ); }
    public void updateString(int columnIndex, String x) throws SQLException { throw notImplemented( "updateString" ); }
    public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw notImplemented( "updateBytes" ); }
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException { throw notImplemented( "updateDate" ); }
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException { throw notImplemented( "updateTime" ); }
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException { throw notImplemented( "updateTimestamp" ); }
    public void updateAsciiStream(int columnIndex, InputStream x, int length ) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateBinaryStream(int columnIndex, InputStream x, int length)  throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateCharacterStream(int columnIndex, java.io.Reader x, int length ) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateObject(int columnIndex, Object x, int scale) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateObject(int columnIndex, Object x) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateNull(String columnName) throws SQLException { throw notImplemented( "updateNull" ); }
	public void updateBoolean(String columnName, boolean x) throws SQLException { throw notImplemented( "updateBoolean" ); }
	public void updateByte(String columnName, byte x) throws SQLException { throw notImplemented( "updateByte" ); }
	public void updateShort(String columnName, short x) throws SQLException { throw notImplemented( "updateShort" ); }
	public void updateInt(String columnName, int x) throws SQLException { throw notImplemented( "updateInt" ); }
	public void updateLong(String columnName, long x) throws SQLException { throw notImplemented( "updateLong" ); }
	public void updateFloat(String columnName, float x) throws SQLException { throw notImplemented( "updateFloat" ); }
	public void updateDouble(String columnName, double x) throws SQLException { throw notImplemented( "updateDouble" ); }
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException { throw notImplemented( "updateBigDecimal" ); }
	public void updateString(String columnName, String x) throws SQLException { throw notImplemented( "updateString" ); }
	public void updateBytes(String columnName, byte[] x) throws SQLException { throw notImplemented( "updateBytes" ); }
	public void updateDate(String columnName, java.sql.Date x) throws SQLException { throw notImplemented( "updateDate" ); }
	public void updateTime(String columnName, java.sql.Time x) throws SQLException { throw notImplemented( "updateTime" ); }
	public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException { throw notImplemented( "updateTimestamp" ); }
	public void updateAsciiStream(String columnName, java.io.InputStream x, int length) throws SQLException { throw notImplemented( "updateAsciiStream" ); }
	public void updateBinaryStream(String columnName, java.io.InputStream x, int length) throws SQLException { throw notImplemented( "updateBinaryStream" ); }
	public void updateCharacterStream(String columnName, java.io.Reader x, int length) throws SQLException { throw notImplemented( "updateCharacterStream" ); }
	public void updateObject(String columnName, Object x, int scale) throws SQLException { throw notImplemented( "updateObject" ); }
	public void updateObject(String columnName, Object x) throws SQLException { throw notImplemented( "updateObject" ); }
	public void insertRow() throws SQLException { throw notImplemented( "insertRow" ); }
	public void updateRow() throws SQLException { throw notImplemented( "updateRow" ); }
	public void deleteRow() throws SQLException { throw notImplemented( "deleteRow" ); }
	public void refreshRow() throws SQLException { throw notImplemented( "refreshRow" ); }
	public void cancelRowUpdates() throws SQLException { throw notImplemented( "cancelRowUpdates" ); }
	public void moveToInsertRow() throws SQLException { throw notImplemented( "moveToInsertRow" ); }
	public void moveToCurrentRow() throws SQLException { throw notImplemented( "moveToCurrentRow" ); }
	public Statement getStatement() throws SQLException { throw notImplemented( "getStatement" ); }
	public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
	public java.sql.Date getDate(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Date getDate" ); }
	public java.sql.Time getTime(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
	public java.sql.Time getTime(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Time getTime" ); }
	public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
	public java.sql.Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException { throw notImplemented( "sql.Timestamp getTimestamp" ); }
	public URL getURL(int columnIndex) throws SQLException { throw notImplemented( "getURL" ); }
	public URL getURL(String columnName) throws SQLException { throw notImplemented( "getURL" ); }
	public Object getObject(int i, java.util.Map map) throws SQLException { throw notImplemented( "getObject" ); }
	public Ref getRef(int i) throws SQLException { throw notImplemented( "getRef" ); }
	public Blob getBlob(int i) throws SQLException { throw notImplemented( "getBlob" ); }
	public Clob getClob(int i) throws SQLException { throw notImplemented( "getClob" ); }
	public Array getArray(int i) throws SQLException { throw notImplemented( "getArray" ); }
	public Object getObject(String colName, java.util.Map map) throws SQLException { throw notImplemented( "getObject" ); }
	public Ref getRef(String colName) throws SQLException { throw notImplemented( "getRef" ); }
	public Blob getBlob(String colName) throws SQLException { throw notImplemented( "getBlob" ); }
	public Clob getClob(String colName) throws SQLException { throw notImplemented( "getClob" ); }
	public Array getArray(String colName) throws SQLException { throw notImplemented( "getArray" ); }
	public void updateRef(int columnIndex, Ref x) throws SQLException { throw notImplemented( "updateRef" ); }
	public void updateRef(String columnName, Ref x) throws SQLException { throw notImplemented( "updateRef" ); }
	public void updateBlob(int columnIndex, Blob x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateBlob(String columnName, Blob x) throws SQLException { throw notImplemented( "updateBlob" ); }
	public void updateClob(int columnIndex, Clob x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateClob(String columnName, Clob x) throws SQLException { throw notImplemented( "updateClob" ); }
	public void updateArray(int columnIndex, Array x) throws SQLException { throw notImplemented( "updateArray" ); }
	public void updateArray(String columnName, Array x) throws SQLException { throw notImplemented( "updateArray" ); }

    public RowId getRowId(int columnIndex) throws SQLException{ throw notImplemented("getRowId"); }
    public RowId getRowId(String columnLabel) throws SQLException{ throw notImplemented("getRowId"); }
    public void updateRowId(int columnIndex,RowId x) throws SQLException{ throw notImplemented("updateRowId"); }
    public void updateRowId(String columnLabel,RowId x) throws SQLException{ throw notImplemented("updateRowId"); }
    public int getHoldability() throws SQLException{ throw notImplemented("getHoldability"); }

    public boolean isClosed() throws SQLException{ throw notImplemented("isClosed"); }
    public void updateNString(int columnIndex,String nString) throws SQLException{throw notImplemented("updateNString");}
    public void updateNString(String columnLabel,String nString) throws SQLException{ throw notImplemented("updateNString"); }
    public void updateNClob(int columnIndex,NClob nClob) throws SQLException{ throw notImplemented("updateNClob"); }
    public void updateNClob(String columnLabel,NClob nClob) throws SQLException{ throw notImplemented("updateNClob"); }
    public NClob getNClob(int columnIndex) throws SQLException{ throw notImplemented("getNClob"); }
    public NClob getNClob(String columnLabel) throws SQLException{ throw notImplemented("getNClob"); }
    public SQLXML getSQLXML(int columnIndex) throws SQLException{ throw notImplemented("getSQLXML"); }
    public SQLXML getSQLXML(String columnLabel) throws SQLException{ throw notImplemented("getSQLXML"); }
    public void updateSQLXML(int columnIndex,SQLXML xmlObject) throws SQLException{ throw notImplemented("updateSQLXML"); }
    public void updateSQLXML(String columnLabel,SQLXML xmlObject) throws SQLException{ throw notImplemented("updateSQLXML"); }
    public String getNString(int columnIndex) throws SQLException{ throw notImplemented("getNString"); }
    public String getNString(String columnLabel) throws SQLException{ throw notImplemented("getNString"); }
    public Reader getNCharacterStream(int columnIndex) throws SQLException{ throw notImplemented("getNCharacterStream"); }
    public Reader getNCharacterStream(String columnLabel) throws SQLException{ throw notImplemented("getNCharacterStream"); }
    public void updateNCharacterStream(int columnIndex,Reader x,long length) throws SQLException{ throw notImplemented("updateNCharacterStream"); }
    public void updateNCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{ throw notImplemented("updateNCharacterStream"); }
    public void updateAsciiStream(int columnIndex,InputStream x,long length) throws SQLException{ throw notImplemented("updateAsciiStream"); }
    public void updateBinaryStream(int columnIndex,InputStream x,long length) throws SQLException{  throw notImplemented("updateBinaryStream"); }
    public void updateCharacterStream(int columnIndex,Reader x,long length) throws SQLException{ throw notImplemented("updateCharacterStream"); }
    public void updateAsciiStream(String columnLabel,InputStream x,long length) throws SQLException{ throw notImplemented("updateAsciiStream"); }
    public void updateBinaryStream(String columnLabel,InputStream x,long length) throws SQLException{ throw notImplemented("updateBinaryStream"); }
    public void updateCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{ throw notImplemented("updateCharacterStream"); }
    public void updateBlob(int columnIndex,InputStream inputStream,long length) throws SQLException{ throw notImplemented("updateBlob"); }
    public void updateBlob(String columnLabel,InputStream inputStream,long length) throws SQLException{ throw notImplemented("updateBlob"); }
    public void updateClob(int columnIndex,Reader reader,long length) throws SQLException{ throw notImplemented("updateClob"); }
    public void updateClob(String columnLabel,Reader reader,long length) throws SQLException{ throw notImplemented("updateClob"); }
    public void updateNClob(int columnIndex,Reader reader,long length) throws SQLException{ throw notImplemented("updateNClob"); }
    public void updateNClob(String columnLabel,Reader reader,long length) throws SQLException{ throw notImplemented("updateNClob"); }
    public void updateNCharacterStream(int columnIndex,Reader x) throws SQLException{ throw notImplemented("updateNCharacterStream"); }
    public void updateNCharacterStream(String columnLabel,Reader reader) throws SQLException{ throw notImplemented("updateNCharacterStream"); }
    public void updateAsciiStream(int columnIndex,InputStream x) throws SQLException{ throw notImplemented("updateAsciiStream"); }
    public void updateBinaryStream(int columnIndex,InputStream x) throws SQLException{ throw notImplemented("updateBinaryStream"); }
    public void updateCharacterStream(int columnIndex,Reader x) throws SQLException{ throw notImplemented("updateCharacterStream"); }
    public void updateAsciiStream(String columnLabel,InputStream x) throws SQLException{ throw notImplemented("updateAsciiStream"); }
    public void updateBinaryStream(String columnLabel,InputStream x) throws SQLException{ throw notImplemented("updateBinaryStream");  }
    public void updateCharacterStream(String columnLabel,Reader reader) throws SQLException{ throw notImplemented("updateCharacterStream"); }
    public void updateBlob(int columnIndex,InputStream inputStream) throws SQLException{ throw notImplemented("updateBlob"); }
    public void updateBlob(String columnLabel,InputStream inputStream) throws SQLException{ throw notImplemented("updateBlob"); }
    public void updateClob(int columnIndex,Reader reader) throws SQLException{ throw notImplemented("updateClob"); }
    public void updateClob(String columnLabel,Reader reader) throws SQLException{ throw notImplemented("updateClob");  }
    public void updateNClob(int columnIndex,Reader reader) throws SQLException{ throw notImplemented("updateNClob"); }
    public void updateNClob(String columnLabel,Reader reader) throws SQLException{ throw notImplemented("updateNClob"); }
    public <T> T getObject(int columnIndex,Class<T> type) throws SQLException{ throw notImplemented("getObject"); }
    public <T> T getObject(String columnLabel,Class<T> type) throws SQLException{ throw notImplemented("getObject"); }
    public <T> T unwrap(Class<T> iface) throws SQLException{ throw notImplemented("unwrap");  }
    public boolean isWrapperFor(Class<?> iface) throws SQLException{ throw notImplemented("isWrapperFor"); }

    /////////////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a SQLException saying that the calling method is not implemented.
     * </p>
     */
    protected SQLException    notImplemented( String methodName )
    {
        return new SQLException( "Unimplemented method: " + methodName );
    }
    
}
