package com.splicemachine.sql;

import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * A ResultSet interface that relies on a Stream to fetch Results.
 *
 * <p>
 *    This is useful when a ResultSet is called for which fetches and parses data from some
 *    unusual stream(such as a network interface or file).
 * </p>
 *
 * <p>
 *     Note that this implementation only supports forward-only result sets (since Streams are forward-only
 *     by design).
 * </p>
 * @author Scott Fines
 *         Date: 1/28/15
 */
public abstract class StreamResultSet extends ReadOnlyResultSet {
    private final Stream<RowResult> stream;
    private final ResultSetMetaData metaData;
    private final String cursorName;
    private RowResult current;
    private RowResult previous;

    private boolean closed = false;
    private boolean exhausted;
    private int rowCount = 0;

    public StreamResultSet(String cursorName,Stream<RowResult> stream, ResultSetMetaData metaData) {
        this.stream = stream;
        this.metaData = metaData;
        this.cursorName = cursorName;
    }

    /*****************************************************************************************************************/
    /*Abstract methods*/
    /**
     * Parse the underlying StreamException into a SQLException.
     *
     * @param e the exception to parse
     * @return a SQLException representing the underlying StreamException error
     */
    protected abstract SQLException parseException(StreamException e);

    /*****************************************************************************************************************/
    /*Management methods*/
    @Override
    public boolean next() throws SQLException {
        try {
            RowResult next = stream.next();
            if(next!=null){
                rowCount++;
                previous = current;
                current = next;
                return true;
            }else {
                exhausted=true;
                return false;
            }
        } catch (StreamException e) {
            throw parseException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        closed=true;
        try {
            stream.close();
        } catch (StreamException e) {
            throw parseException(e);
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int columnCount = metaData.getColumnCount();
        for(int i=1;i<= columnCount;i++){
           if(metaData.getColumnLabel(i).equalsIgnoreCase(columnLabel)) return columnCount;
        }
        throw new SQLException("Cannot find column with label <"+columnLabel+">");
    }

    @Override public ResultSetMetaData getMetaData() throws SQLException { return metaData; }

    /***********************************************************************************************************/
    /*Warning methods*/
    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override public String getCursorName() throws SQLException { return cursorName; }
    @Override public int getType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean isClosed() throws SQLException { return closed; }
    @Override public int getHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    /*Supported row position methods*/
    @Override public boolean isBeforeFirst() throws SQLException { return previous==null && current==null;  }
    @Override public boolean isAfterLast() throws SQLException { return exhausted; }
    @Override public boolean isFirst() throws SQLException { return previous==null && current!=null; }
    @Override public int getRow() throws SQLException { return rowCount; }

    /*****************************************************************************************************************/
    /*Accessor methods*/

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return current.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        return current.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        return current.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        return current.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        return current.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        return current.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        return current.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        return current.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        return current.getDouble(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkClosed();
        return current.getBigDecimal(columnIndex,scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        return current.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        return current.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        return current.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        return current.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkClosed();
        return current.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkClosed();
        return current.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkClosed();
        return current.getBinaryStream(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        return current.getObject(columnIndex);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        return current.getCharacterStream(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        return current.getBigDecimal(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        return current.getObject(columnIndex,map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        checkClosed();
        return current.getRef(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        checkClosed();
        return current.getBlob(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        checkClosed();
        return current.getClob(columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkClosed();
        return current.getArray(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        return current.getDate(columnIndex, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        return current.getTime(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkClosed();
        return current.getTimestamp(columnIndex, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkClosed();
        return current.getURL(columnIndex);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        checkClosed();
        return current.getRowId(columnIndex);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        checkClosed();
        return current.getNClob(columnIndex);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        checkClosed();
        return current.getSQLXML(columnIndex);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        checkClosed();
        return current.getNString(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        checkClosed();
        return current.getNCharacterStream(columnIndex);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkClosed();
        return current.getObject(columnIndex, type);
    }

    /*****************************************************************************************************************/
    /*Unsupported methods*/
    @Override
    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException("No Statement available for a StreamResultSet");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException("Unable to determine lastability with a generic stream");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException("Forward-only cursor");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Forward-only cursor");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Forward-only cursor");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException("Unseekable cursor");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Cannot unwrap this result set");
    }

    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private void checkClosed() throws SQLException {
        if(closed) throw new SQLException("ResultSet was closed");
    }
}
