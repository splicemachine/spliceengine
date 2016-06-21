package com.splicemachine.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;


/**
 * Representation of a Row of SQL Data.
 *
 * @author Scott Fines
 *         Date: 1/28/15
 */
public interface RowResult {
    boolean wasNull()throws SQLException;

    String getString(int columnIndex)throws SQLException;

    boolean getBoolean(int columnIndex)throws SQLException;

    byte getByte(int columnIndex)throws SQLException;

    short getShort(int columnIndex)throws SQLException;

    int getInt(int columnIndex)throws SQLException;

    long getLong(int columnIndex)throws SQLException;

    float getFloat(int columnIndex)throws SQLException;

    double getDouble(int columnIndex)throws SQLException;

    BigDecimal getBigDecimal(int columnIndex)throws SQLException;

    byte[] getBytes(int columnIndex)throws SQLException;

    Date getDate(int columnIndex)throws SQLException;

    Time getTime(int columnIndex)throws SQLException;

    Timestamp getTimestamp(int columnIndex)throws SQLException;

    InputStream getAsciiStream(int columnIndex)throws SQLException;

    InputStream getUnicodeStream(int columnIndex)throws SQLException;

    InputStream getBinaryStream(int columnIndex)throws SQLException;

    Object getObject(int columnIndex)throws SQLException;

    Reader getCharacterStream(int columnIndex)throws SQLException;

    BigDecimal getBigDecimal(int columnIndex, int scale)throws SQLException;

    Object getObject(int columnIndex, Map<String, Class<?>> map)throws SQLException;

    <T> T getObject(int columnIndex, Class<T> type)throws SQLException;

    Ref getRef(int columnIndex)throws SQLException;

    Blob getBlob(int columnIndex)throws SQLException;

    Clob getClob(int columnIndex)throws SQLException;

    Array getArray(int columnIndex)throws SQLException;

    Date getDate(int columnIndex, Calendar cal)throws SQLException;

    Time getTime(int columnIndex, Calendar cal)throws SQLException;

    Timestamp getTimestamp(int columnIndex, Calendar cal)throws SQLException;

    URL getURL(int columnIndex)throws SQLException;

    RowId getRowId(int columnIndex)throws SQLException;

    NClob getNClob(int columnIndex)throws SQLException;

    SQLXML getSQLXML(int columnIndex)throws SQLException;

    String getNString(int columnIndex)throws SQLException;

    Reader getNCharacterStream(int columnIndex)throws SQLException;
}
