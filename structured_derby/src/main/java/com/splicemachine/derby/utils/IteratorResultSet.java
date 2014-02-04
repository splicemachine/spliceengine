package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 2/4/14
 */
public class IteratorResultSet implements ResultSet {
		private final List<ExecRow> results;
		private final ObjectIntOpenHashMap<String> columnIdMap;

		private Iterator<ExecRow> nextRow;
		private ExecRow currentRow;
		private DataValueDescriptor lastColumnAccessed;
		private int position= 1;

		public IteratorResultSet(List<ExecRow> results, ObjectIntOpenHashMap<String> columnIdMap) {
				this.results = results;
				this.columnIdMap = columnIdMap;
		}

		@Override
		public boolean next() throws SQLException {
				if(!nextRow.hasNext()) return false;
				position++;
				currentRow = nextRow.next();
				return true;
		}

		@Override public void close() throws SQLException { nextRow = null; }

		@Override
		public boolean wasNull() throws SQLException {
				return lastColumnAccessed==null || lastColumnAccessed.isNull();
		}

		@Override
		public String getString(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null) return null;
						return lastColumnAccessed.getString();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public boolean getBoolean(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						return !(lastColumnAccessed == null || lastColumnAccessed.isNull()) && lastColumnAccessed.getBoolean();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public byte getByte(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return (byte)0;
						return lastColumnAccessed.getByte();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public short getShort(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return 0;
						return lastColumnAccessed.getShort();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public int getInt(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return 0;
						return lastColumnAccessed.getInt();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public long getLong(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return 0l;
						return lastColumnAccessed.getLong();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public float getFloat(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return 0f;
						return lastColumnAccessed.getFloat();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public double getDouble(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return 0d;
						return lastColumnAccessed.getDouble();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return (BigDecimal)lastColumnAccessed.getObject();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public byte[] getBytes(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getBytes();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public Date getDate(int columnIndex) throws SQLException {
				return getDate(columnIndex,null);
		}

		@Override
		public Time getTime(int columnIndex) throws SQLException {
				return getTime(columnIndex,null);
		}

		@Override
		public Timestamp getTimestamp(int columnIndex) throws SQLException {
				return getTimestamp(columnIndex,null);
		}

		@Override
		public InputStream getAsciiStream(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getStream();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public InputStream getUnicodeStream(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getStream();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public InputStream getBinaryStream(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getStream();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public String getString(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getString(columnIdMap.get(columnLabel));
		}

		@Override
		public boolean getBoolean(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getBoolean(columnIdMap.get(columnLabel));
		}

		@Override
		public byte getByte(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getByte(columnIdMap.get(columnLabel));
		}

		@Override
		public short getShort(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getShort(columnIdMap.get(columnLabel));
		}

		@Override
		public int getInt(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getInt(columnIdMap.get(columnLabel));
		}

		@Override
		public long getLong(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getLong(columnIdMap.get(columnLabel));
		}

		@Override
		public float getFloat(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getFloat(columnIdMap.get(columnLabel));
		}

		@Override
		public double getDouble(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getDouble(columnIdMap.get(columnLabel));
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getBigDecimal(columnIdMap.get(columnLabel));
		}

		@Override
		public byte[] getBytes(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getBytes(columnIdMap.get(columnLabel));
		}

		@Override
		public Date getDate(String columnLabel) throws SQLException {
				return getDate(columnLabel,null);
		}

		@Override
		public Time getTime(String columnLabel) throws SQLException {
				return getTime(columnLabel,null);
		}

		@Override
		public Timestamp getTimestamp(String columnLabel) throws SQLException {
				return getTimestamp(columnLabel,null);
		}

		@Override
		public InputStream getAsciiStream(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getAsciiStream(columnIdMap.get(columnLabel));
		}

		@Override
		public InputStream getUnicodeStream(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getUnicodeStream(columnIdMap.get(columnLabel));
		}

		@Override
		public InputStream getBinaryStream(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getBinaryStream(columnIdMap.get(columnLabel));
		}

		@Override public SQLWarning getWarnings() throws SQLException { return null; }

		@Override public void clearWarnings() throws SQLException {  }

		@Override public String getCursorName() throws SQLException { return null; }

		@Override
		public ResultSetMetaData getMetaData() throws SQLException {

				return null;
		}

		@Override
		public Object getObject(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getObject();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public Object getObject(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getObject(columnIdMap.get(columnLabel));
		}

		@Override
		public int findColumn(String columnLabel) throws SQLException {
				return columnIdMap.get(columnLabel);
		}

		@Override
		public Reader getCharacterStream(int columnIndex) throws SQLException {
				return new InputStreamReader(getUnicodeStream(columnIndex));
		}

		@Override
		public Reader getCharacterStream(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getCharacterStream(columnIdMap.get(columnLabel));
		}

		@Override
		public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return (BigDecimal)lastColumnAccessed.getObject();
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getBigDecimal(columnIdMap.get(columnLabel));
		}

		@Override
		public boolean isBeforeFirst() throws SQLException {
				return nextRow==null;
		}

		@Override public boolean isAfterLast() throws SQLException { return !nextRow.hasNext(); }

		@Override public boolean isFirst() throws SQLException { return lastColumnAccessed==null; }

		@Override public boolean isLast() throws SQLException { return nextRow !=null && !nextRow.hasNext(); }

		@Override public void beforeFirst() throws SQLException { nextRow = results.iterator(); }

		@Override public void afterLast() throws SQLException { nextRow = null; }

		@Override
		public boolean first() throws SQLException {
				nextRow = results.iterator();
				return true;
		}

		@Override
		public boolean last() throws SQLException {
				currentRow = results.get(results.size()-1);
				nextRow=null;
				return true;
		}

		@Override
		public int getRow() throws SQLException {
				return position;
		}

		@Override public boolean absolute(int row) throws SQLException { return false; }
		@Override public boolean relative(int rows) throws SQLException { return false; }
		@Override public boolean previous() throws SQLException { return false; }
		@Override public void setFetchDirection(int direction) throws SQLException {  }
		@Override public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }
		@Override public void setFetchSize(int rows) throws SQLException {  }
		@Override public int getFetchSize() throws SQLException { return 0; }
		@Override public int getType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }
		@Override public int getConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }
		@Override public boolean rowUpdated() throws SQLException { return false; }
		@Override public boolean rowInserted() throws SQLException { return false; }
		@Override public boolean rowDeleted() throws SQLException { return false; }
		@Override public void updateNull(int columnIndex) throws SQLException {  }
		@Override public void updateBoolean(int columnIndex, boolean x) throws SQLException {  }
		@Override public void updateByte(int columnIndex, byte x) throws SQLException {  }
		@Override public void updateShort(int columnIndex, short x) throws SQLException {  }
		@Override public void updateInt(int columnIndex, int x) throws SQLException {  }
		@Override public void updateLong(int columnIndex, long x) throws SQLException {  }
		@Override public void updateFloat(int columnIndex, float x) throws SQLException {  }
		@Override public void updateDouble(int columnIndex, double x) throws SQLException {  }
		@Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {  }
		@Override public void updateString(int columnIndex, String x) throws SQLException {  }
		@Override public void updateBytes(int columnIndex, byte[] x) throws SQLException {  }
		@Override public void updateDate(int columnIndex, Date x) throws SQLException {  }

		@Override
		public void updateTime(int columnIndex, Time x) throws SQLException {

		}

		@Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {  }
		@Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {  }
		@Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {  }
		@Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {  }
		@Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {  }
		@Override public void updateObject(int columnIndex, Object x) throws SQLException {  }
		@Override public void updateNull(String columnLabel) throws SQLException {  }
		@Override public void updateBoolean(String columnLabel, boolean x) throws SQLException {  }
		@Override public void updateByte(String columnLabel, byte x) throws SQLException {  }
		@Override public void updateShort(String columnLabel, short x) throws SQLException {  }
		@Override public void updateInt(String columnLabel, int x) throws SQLException {  }
		@Override public void updateLong(String columnLabel, long x) throws SQLException {  }
		@Override public void updateFloat(String columnLabel, float x) throws SQLException {  }
		@Override public void updateDouble(String columnLabel, double x) throws SQLException {  }
		@Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {  }
		@Override public void updateString(String columnLabel, String x) throws SQLException {  }
		@Override public void updateBytes(String columnLabel, byte[] x) throws SQLException {  }
		@Override public void updateDate(String columnLabel, Date x) throws SQLException {  }
		@Override public void updateTime(String columnLabel, Time x) throws SQLException {  }
		@Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {  }
		@Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {  }
		@Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {  }
		@Override public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {  }
		@Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {  }
		@Override public void updateObject(String columnLabel, Object x) throws SQLException {  }
		@Override public void insertRow() throws SQLException {  }
		@Override public void updateRow() throws SQLException {  }
		@Override public void deleteRow() throws SQLException {  }
		@Override public void refreshRow() throws SQLException {  }
		@Override public void cancelRowUpdates() throws SQLException {  }
		@Override public void moveToInsertRow() throws SQLException {  }
		@Override public void moveToCurrentRow() throws SQLException {  }
		@Override public Statement getStatement() throws SQLException { return null; }

		@Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
				return getObject(columnIndex);
		}

		@Override public Ref getRef(int columnIndex) throws SQLException { return null; }
		@Override public Blob getBlob(int columnIndex) throws SQLException { return null; }
		@Override public Clob getClob(int columnIndex) throws SQLException { return null; }
		@Override public Array getArray(int columnIndex) throws SQLException { return null; }

		@Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getObject(columnIdMap.get(columnLabel), map);
		}

		@Override public Ref getRef(String columnLabel) throws SQLException { return null; }
		@Override public Blob getBlob(String columnLabel) throws SQLException { return null; }
		@Override public Clob getClob(String columnLabel) throws SQLException { return null; }
		@Override public Array getArray(String columnLabel) throws SQLException { return null; }

		@Override public Date getDate(int columnIndex, Calendar cal) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getDate(cal);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override public Date getDate(String columnLabel, Calendar cal) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getDate(columnIdMap.get(columnLabel),null);
		}

		@Override
		public Time getTime(int columnIndex, Calendar cal) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getTime(cal);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public Time getTime(String columnLabel, Calendar cal) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getTime(columnIdMap.get(columnLabel),cal);
		}

		@Override
		public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
				try {
						lastColumnAccessed = currentRow.getColumn(columnIndex);
						if(lastColumnAccessed==null||lastColumnAccessed.isNull()) return null;
						return lastColumnAccessed.getTimestamp(cal);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

		@Override
		public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
				if(!columnIdMap.containsKey(columnLabel))
						throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_NOT_FOUND.newException(columnLabel));
				return getTimestamp(columnIdMap.get(columnLabel),cal);
		}

		@Override public URL getURL(int columnIndex) throws SQLException { return null; }
		@Override public URL getURL(String columnLabel) throws SQLException { return null; }
		@Override public void updateRef(int columnIndex, Ref x) throws SQLException {  }
		@Override public void updateRef(String columnLabel, Ref x) throws SQLException {  }
		@Override public void updateBlob(int columnIndex, Blob x) throws SQLException {  }
		@Override public void updateBlob(String columnLabel, Blob x) throws SQLException {  }
		@Override public void updateClob(int columnIndex, Clob x) throws SQLException {  }
		@Override public void updateClob(String columnLabel, Clob x) throws SQLException {  }
		@Override public void updateArray(int columnIndex, Array x) throws SQLException {  }
		@Override public void updateArray(String columnLabel, Array x) throws SQLException {  }
		@Override public RowId getRowId(int columnIndex) throws SQLException { return null; }
		@Override public RowId getRowId(String columnLabel) throws SQLException { return null; }
		@Override public void updateRowId(int columnIndex, RowId x) throws SQLException {  }
		@Override public void updateRowId(String columnLabel, RowId x) throws SQLException {  }
		@Override public int getHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
		@Override public boolean isClosed() throws SQLException { return false; }
		@Override public void updateNString(int columnIndex, String nString) throws SQLException {  }
		@Override public void updateNString(String columnLabel, String nString) throws SQLException {  }
		@Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException {  }
		@Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException {  }
		@Override public NClob getNClob(int columnIndex) throws SQLException { return null; }
		@Override public NClob getNClob(String columnLabel) throws SQLException { return null; }
		@Override public SQLXML getSQLXML(int columnIndex) throws SQLException { return null; }
		@Override public SQLXML getSQLXML(String columnLabel) throws SQLException { return null; }
		@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {  }
		@Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {  }
		@Override public String getNString(int columnIndex) throws SQLException { return null; }
		@Override public String getNString(String columnLabel) throws SQLException { return null; }
		@Override public Reader getNCharacterStream(int columnIndex) throws SQLException { return null; }
		@Override public Reader getNCharacterStream(String columnLabel) throws SQLException { return null; }
		@Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {  }
		@Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {  }
		@Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {  }
		@Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {  }
		@Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {  }
		@Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {  }
		@Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {  }
		@Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {  }
		@Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {  }
		@Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {  }
		@Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {  }
		@Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {  }
		@Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {  }
		@Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {  }
		@Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {  }
		@Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {  }
		@Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {  }
		@Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {  }
		@Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {  }
		@Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {  }
		@Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {  }
		@Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {  }
		@Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {  }
		@Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {  }
		@Override public void updateClob(int columnIndex, Reader reader) throws SQLException {  }
		@Override public void updateClob(String columnLabel, Reader reader) throws SQLException {  }
		@Override public void updateNClob(int columnIndex, Reader reader) throws SQLException {  }
		@Override public void updateNClob(String columnLabel, Reader reader) throws SQLException {  }

		public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
				return type.cast(getObject(columnIndex));
		}

		public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
				return type.cast(getObject(columnLabel));
		}

		@Override public <T> T unwrap(Class<T> iface) throws SQLException { return null; }
		@Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
