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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;

import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.dbTesting.junit.JDBC;

/**
    This class has been adapted from com.splicemachine.db.vti.VTITemplate
    because we do not want a test to depend on an engine class.
   
    This class implements most of the methods of the JDBC 1.2 interface java.sql.ResultSet,
    each one throwing a  SQLException with the name of the method. 
    A concrete subclass can then just implement the methods not implemented here 
    and override any methods it needs to implement for correct functionality.
    <P>
    The methods not implemented here are
    <UL>
    <LI>next()
    <LI>close()
    <LI>getMetaData()
    </UL>
    <P>

    For virtual tables the database engine only calls methods defined
    in the JDBC 1.2 definition of java.sql.ResultSet.
    <BR>
    Classes that implement a JDBC 2.0 conformant java.sql.ResultSet can be used
    as virtual tables.
*/
public abstract class TableVTI implements ResultSet {
    
    private String tableName;
    private ResultSetMetaData rsmd ;
    private int[] map;
    public TableVTI(String tableName) throws SQLException {
        this.tableName = tableName;
        init();
    }
    
    protected void init() throws SQLException {
        Connection conn = DriverManager
        .getConnection("jdbc:default:connection");
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select * from " + JDBC.escape(tableName)
                + " where 1 = 0 ");
        rsmd = rs.getMetaData();
        rs.close();
        s.close();
        
        map = getColumnMap(getVTIColumnNames());
    }
    
    public final ResultSetMetaData getMetaData() {
        return rsmd;
    }
    
    public final void close() {
    }
    
    protected int[] getColumnMap(String[] vtiNames) throws SQLException {
        
        if (vtiNames == null)
            return null;
        
        int[] map = new int[vtiNames.length];
        int count = rsmd.getColumnCount();
        
        outer: for (int i = 0; i < vtiNames.length; i++) {
            
            String vtiCol = vtiNames[i];
            if (vtiCol == null)
                continue outer;
            ;
            
            for (int j = 1; j <= count; j++) {
                if (vtiCol.equalsIgnoreCase(rsmd.getColumnName(j))) {
                    map[i] = j;
                    continue outer;
                }
            }
        }
        
        return map;
    }
    
    protected String[] getVTIColumnNames() {
        return null;
    }
    
    //
    // java.sql.ResultSet calls, passed through to our result set.
    //
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean wasNull() throws SQLException {
        throw new SQLException("wasNull");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public String getString(int columnIndex) throws SQLException {
        throw new SQLException("getString");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        throw new SQLException("getBoolean");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public byte getByte(int columnIndex) throws SQLException {
        throw new SQLException("getByte");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public short getShort(int columnIndex) throws SQLException {
        throw new SQLException("getShort");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getInt(int columnIndex) throws SQLException {
        throw new SQLException("getInt");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public long getLong(int columnIndex) throws SQLException {
        throw new SQLException("getLong");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public float getFloat(int columnIndex) throws SQLException {
        throw new SQLException("getFloat");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public double getDouble(int columnIndex) throws SQLException {
        throw new SQLException("getDouble");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLException("getBigDecimal");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLException("getBytes");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        throw new SQLException("getDate");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Time getTime(int columnIndex) throws SQLException {
        throw new SQLException("getTime");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLException("getTimestamp");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLException("getAsciiStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLException("getUnicodeStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getBinaryStream(int columnIndex)
    throws SQLException {
        throw new SQLException("getBinaryStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnName),scale);
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        throw new SQLException("getAsciiStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
        throw new SQLException("getUnicodeStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.InputStream getBinaryStream(String columnName)
    throws SQLException {
        throw new SQLException("getBinaryStream");
    }
    
    /**
     * @exception    SQLException if there is an error
     */
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    /**
     * @exception    SQLException if there is an error
     */
    public void clearWarnings() throws SQLException {
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public String getCursorName() throws SQLException {
        throw new SQLException("getCursorName");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Object getObject(int columnIndex) throws SQLException {
        throw new SQLException("getObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int findColumn(String columnName) throws SQLException {
        throw new SQLException("findColumn");
    }
    
    /*
     ** JDBC 2.0 methods
     */
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.Reader getCharacterStream(int columnIndex)
    throws SQLException {
        throw new SQLException("getCharacterStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.io.Reader getCharacterStream(String columnName)
    throws SQLException {
        throw new SQLException("getCharacterStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public BigDecimal getBigDecimal(int columnIndex)
    throws SQLException {
        throw new SQLException("getBigDecimal");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public BigDecimal getBigDecimal(String columnName)
    throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean isBeforeFirst()
    throws SQLException {
        throw new SQLException("isBeforeFirst");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean isAfterLast()
    throws SQLException {
        throw new SQLException("isAfterLast");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean isFirst()
    throws SQLException {
        throw new SQLException("isFirst");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean isLast()
    throws SQLException {
        throw new SQLException("isLast");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void beforeFirst()
    throws SQLException {
        throw new SQLException("beforeFirst");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void afterLast()
    throws SQLException {
        throw new SQLException("afterLast");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean first()
    throws SQLException {
        throw new SQLException("first");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean last()
    throws SQLException {
        throw new SQLException("last");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getRow()
    throws SQLException {
        throw new SQLException("getRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean absolute(int row)
    throws SQLException {
        throw new SQLException("absolute");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean relative(int rows)
    throws SQLException {
        throw new SQLException("relative");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean previous()
    throws SQLException {
        throw new SQLException("previous");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void setFetchDirection(int direction)
    throws SQLException {
        throw new SQLException("setFetchDirection");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getFetchDirection()
    throws SQLException {
        throw new SQLException("getFetchDirection");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void setFetchSize(int rows)
    throws SQLException {
        throw new SQLException("setFetchSize");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getFetchSize()
    throws SQLException {
        throw new SQLException("getFetchSize");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getType()
    throws SQLException {
        throw new SQLException("getType");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public int getConcurrency()
    throws SQLException {
        throw new SQLException("getConcurrency");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean rowUpdated()
    throws SQLException {
        throw new SQLException("rowUpdated");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean rowInserted()
    throws SQLException {
        throw new SQLException("rowInserted");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public boolean rowDeleted()
    throws SQLException {
        throw new SQLException("rowDeleted");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateNull(int columnIndex)
    throws SQLException {
        throw new SQLException("updateNull");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBoolean(int columnIndex, boolean x)
    throws SQLException {
        throw new SQLException("updateBoolean");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateByte(int columnIndex, byte x)
    throws SQLException {
        throw new SQLException("updateByte");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateShort(int columnIndex, short x)
    throws SQLException {
        throw new SQLException("updateShort");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateInt(int columnIndex, int x)
    throws SQLException {
        throw new SQLException("updateInt");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateLong(int columnIndex, long x)
    throws SQLException {
        throw new SQLException("updateLong");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateFloat(int columnIndex, float x)
    throws SQLException {
        throw new SQLException("updateFloat");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateDouble(int columnIndex, double x)
    throws SQLException {
        throw new SQLException("updateDouble");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x)
    throws SQLException {
        throw new SQLException("updateBigDecimal");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateString(int columnIndex, String x)
    throws SQLException {
        throw new SQLException("updateString");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBytes(int columnIndex, byte[] x)
    throws SQLException {
        throw new SQLException("updateBytes");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateDate(int columnIndex, java.sql.Date x)
    throws SQLException {
        throw new SQLException("updateDate");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateTime(int columnIndex, java.sql.Time x)
    throws SQLException {
        throw new SQLException("updateTime");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
    throws SQLException {
        throw new SQLException("updateTimestamp");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateAsciiStream(int columnIndex,
            java.io.InputStream x,
            int length)
    throws SQLException {
        throw new SQLException("updateAsciiStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBinaryStream(int columnIndex,
            java.io.InputStream x,
            int length)
    throws SQLException {
        throw new SQLException("updateBinaryStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateCharacterStream(int columnIndex,
            java.io.Reader x,
            int length)
    throws SQLException {
        throw new SQLException("updateCharacterStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateObject(int columnIndex,
            Object x,
            int scale)
    throws SQLException {
        throw new SQLException("updateObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateObject(int columnIndex, Object x)
    throws SQLException {
        throw new SQLException("updateObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateNull(String columnName)
    throws SQLException {
        throw new SQLException("updateNull");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBoolean(String columnName, boolean x)
    throws SQLException {
        throw new SQLException("updateBoolean");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateByte(String columnName, byte x)
    throws SQLException {
        throw new SQLException("updateByte");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateShort(String columnName, short x)
    throws SQLException {
        throw new SQLException("updateShort");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateInt(String columnName, int x)
    throws SQLException {
        throw new SQLException("updateInt");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateLong(String columnName, long x)
    throws SQLException {
        throw new SQLException("updateLong");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateFloat(String columnName, float x)
    throws SQLException {
        throw new SQLException("updateFloat");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateDouble(String columnName, double x)
    throws SQLException {
        throw new SQLException("updateDouble");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBigDecimal(String columnName, BigDecimal x)
    throws SQLException {
        throw new SQLException("updateBigDecimal");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateString(String columnName, String x)
    throws SQLException {
        throw new SQLException("updateString");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBytes(String columnName, byte[] x)
    throws SQLException {
        throw new SQLException("updateBytes");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateDate(String columnName, java.sql.Date x)
    throws SQLException {
        throw new SQLException("updateDate");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateTime(String columnName, java.sql.Time x)
    throws SQLException {
        throw new SQLException("updateTime");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateTimestamp(String columnName, java.sql.Timestamp x)
    throws SQLException {
        throw new SQLException("updateTimestamp");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateAsciiStream(String columnName,
            java.io.InputStream x,
            int length)
    throws SQLException {
        throw new SQLException("updateAsciiStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBinaryStream(String columnName,
            java.io.InputStream x,
            int length)
    throws SQLException {
        throw new SQLException("updateBinaryStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateCharacterStream(String columnName,
            java.io.Reader x,
            int length)
    throws SQLException {
        throw new SQLException("updateCharacterStream");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateObject(String columnName,
            Object x,
            int scale)
    throws SQLException {
        throw new SQLException("updateObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateObject(String columnName, Object x)
    throws SQLException {
        throw new SQLException("updateObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void insertRow()
    throws SQLException {
        throw new SQLException("insertRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateRow()
    throws SQLException {
        throw new SQLException("updateRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void deleteRow()
    throws SQLException {
        throw new SQLException("deleteRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void refreshRow()
    throws SQLException {
        throw new SQLException("refreshRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void cancelRowUpdates()
    throws SQLException {
        throw new SQLException("cancelRowUpdates");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void moveToInsertRow()
    throws SQLException {
        throw new SQLException("moveToInsertRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void moveToCurrentRow()
    throws SQLException {
        throw new SQLException("moveToCurrentRow");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Statement getStatement()
    throws SQLException {
        throw new SQLException("getStatement");
    }
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Date getDate(int columnIndex, Calendar cal)
    throws SQLException {
        throw new SQLException("getDate");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Date getDate(String columnName, Calendar cal)
    throws SQLException {
        throw new SQLException("getDate");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Time getTime(int columnIndex, Calendar cal)
    throws SQLException {
        throw new SQLException("getTime");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Time getTime(String columnName, Calendar cal)
    throws SQLException {
        throw new SQLException("getTime");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal)
    throws SQLException {
        throw new SQLException("getTimestamp");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)
    throws SQLException {
        throw new SQLException("getTimestamp");
    }
    /*
     ** JDBC 3.0 methods
     */
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public URL getURL(int columnIndex)
    throws SQLException
    {
        throw new SQLException("getURL");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public URL getURL(String columnName)
    throws SQLException {
        throw new SQLException("getURL");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Object getObject(int i, java.util.Map map)
    throws SQLException {
        throw new SQLException("getObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Ref getRef(int i)
    throws SQLException {
        throw new SQLException("getRef");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Blob getBlob(int i)
    throws SQLException {
        throw new SQLException("getBlob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Clob getClob(int i)
    throws SQLException {
        throw new SQLException("getClob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Array getArray(int i)
    throws SQLException {
        throw new SQLException("getArray");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Object getObject(String colName, java.util.Map map)
    throws SQLException {
        throw new SQLException("getObject");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Ref getRef(String colName)
    throws SQLException {
        throw new SQLException("getRef");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Blob getBlob(String colName)
    throws SQLException {
        throw new SQLException("getBlob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Clob getClob(String colName)
    throws SQLException {
        throw new SQLException("getClob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public Array getArray(String colName)
    throws SQLException {
        throw new SQLException("getArray");
    }
    
    
    // JDBC 3.0 methods - not implemented
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateRef(int columnIndex, Ref x)
    throws SQLException {
        throw new SQLException("updateRef");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateRef(String columnName, Ref x)
    throws SQLException {
        throw new SQLException("updateRef");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBlob(int columnIndex, Blob x)
    throws SQLException {
        throw new SQLException("updateBlob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateBlob(String columnName, Blob x)
    throws SQLException {
        throw new SQLException("updateBlob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateClob(int columnIndex, Clob x)
    throws SQLException {
        throw new SQLException("updateClob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateClob(String columnName, Clob x)
    throws SQLException {
        throw new SQLException("updateClob");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateArray(int columnIndex, Array x)
    throws SQLException {
        throw new SQLException("updateArray");
    }
    
    /**
     * @see java.sql.ResultSet
     *
     * @exception SQLException on unexpected JDBC error
     */
    public void updateArray(String columnName, Array x)
    throws SQLException {
        throw new SQLException("updateArray");
    }


    @Override
    public RowId getRowId(int columnIndex) throws SQLException{
        throw Util.notImplemented("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException{
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex,RowId x) throws SQLException{
        throw Util.notImplemented("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel,RowId x) throws SQLException{
        updateRowId(findColumn(columnLabel),x);
    }

    @Override
    public int getHoldability() throws SQLException{
        throw Util.notImplemented("getHoldability");
    }

    @Override
    public boolean isClosed() throws SQLException{
        throw Util.notImplemented("isClosed");
    }

    @Override
    public void updateNString(int columnIndex,String nString) throws SQLException{
        throw Util.notImplemented("updateNString");
    }

    @Override
    public void updateNString(String columnLabel,String nString) throws SQLException{
        updateNString(findColumn(columnLabel),nString);
    }

    @Override
    public void updateNClob(int columnIndex,NClob nClob) throws SQLException{
        throw Util.notImplemented("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel,NClob nClob) throws SQLException{
        updateNClob(findColumn(columnLabel),nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException{
        throw Util.notImplemented("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException{
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException{
        throw Util.notImplemented("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException{
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex,SQLXML xmlObject) throws SQLException{
        throw Util.notImplemented("UpdateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel,SQLXML xmlObject) throws SQLException{
        updateSQLXML(findColumn(columnLabel),xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException{
        throw Util.notImplemented("getNString");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException{
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException{
        throw Util.notImplemented("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException{
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex,Reader x,long length) throws SQLException{
        throw Util.notImplemented("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{
        updateNCharacterStream(findColumn(columnLabel),reader,length);
    }

    @Override
    public void updateAsciiStream(int columnIndex,InputStream x,long length) throws SQLException{
        throw Util.notImplemented("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex,InputStream x,long length) throws SQLException{
        throw Util.notImplemented("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex,Reader x,long length) throws SQLException{
        throw Util.notImplemented("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel,InputStream x,long length) throws SQLException{
        updateAsciiStream(findColumn(columnLabel),x,length);
    }

    @Override
    public void updateBinaryStream(String columnLabel,InputStream x,long length) throws SQLException{
        updateBinaryStream(findColumn(columnLabel),x,length);
    }

    @Override
    public void updateCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{
        updateCharacterStream(findColumn(columnLabel),reader,length);
    }

    @Override
    public void updateBlob(int columnIndex,InputStream inputStream,long length) throws SQLException{
        throw Util.notImplemented("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel,InputStream inputStream,long length) throws SQLException{
        updateBlob(findColumn(columnLabel),inputStream,length);
    }

    @Override
    public void updateClob(int columnIndex,Reader reader,long length) throws SQLException{
        throw Util.notImplemented("updateClob");
    }

    @Override
    public void updateClob(String columnLabel,Reader reader,long length) throws SQLException{
        updateClob(findColumn(columnLabel),reader,length);
    }

    @Override
    public void updateNClob(int columnIndex,Reader reader,long length) throws SQLException{
        throw Util.notImplemented("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel,Reader reader,long length) throws SQLException{
        updateNClob(findColumn(columnLabel),reader,length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex,Reader x) throws SQLException{
        throw Util.notImplemented("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel,Reader reader) throws SQLException{
        updateNCharacterStream(findColumn(columnLabel),reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex,InputStream x) throws SQLException{
        throw Util.notImplemented("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex,InputStream x) throws SQLException{
        throw Util.notImplemented("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex,Reader x) throws SQLException{
        throw Util.notImplemented("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel,InputStream x) throws SQLException{
        updateAsciiStream(findColumn(columnLabel),x);
    }

    @Override
    public void updateBinaryStream(String columnLabel,InputStream x) throws SQLException{
        updateBinaryStream(findColumn(columnLabel),x);
    }

    @Override
    public void updateCharacterStream(String columnLabel,Reader reader) throws SQLException{
        updateCharacterStream(findColumn(columnLabel),reader);
    }

    @Override
    public void updateBlob(int columnIndex,InputStream inputStream) throws SQLException{
        throw Util.notImplemented("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel,InputStream inputStream) throws SQLException{
        updateBlob(findColumn(columnLabel),inputStream);
    }

    @Override
    public void updateClob(int columnIndex,Reader reader) throws SQLException{
        throw Util.notImplemented("updateClob");
    }

    @Override
    public void updateClob(String columnLabel,Reader reader) throws SQLException{
        updateClob(findColumn(columnLabel),reader);
    }

    @Override
    public void updateNClob(int columnIndex,Reader reader) throws SQLException{
        throw Util.notImplemented("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel,Reader reader) throws SQLException{
        updateNClob(findColumn(columnLabel),reader);
    }

    @Override
    public <T> T getObject(int columnIndex,Class<T> type) throws SQLException{
        throw Util.notImplemented("getObject");
    }

    @Override
    public <T> T getObject(String columnLabel,Class<T> type) throws SQLException{
        return getObject(findColumn(columnLabel),type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw Util.notImplemented("unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        throw Util.notImplemented("isWrapperFor");
    }
}
