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
 *         Date: 9/19/16
 */
class ClusteredResultSet implements ResultSet{
    private final ResultSet delegate;
    private final ClusteredStatement source;
//    private final StatementResults statementResults;

    //    ClusteredResultSet(ResultSet n,StatementResults statementResults){
    ClusteredResultSet(ResultSet n,ClusteredStatement source){
        this.delegate=n;
        this.source = source;
//        this.statementResults=statementResults;
    }

    @Override
    public boolean next() throws SQLException{
        try{
            return delegate.next();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void close() throws SQLException{
        try{
            delegate.close();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException{
        try{
            return delegate.wasNull();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException{
        try{
            return delegate.getString(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException{
        try{
            return delegate.getBoolean(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException{
        try{
            return delegate.getByte(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException{
        try{
            return delegate.getShort(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException{
        try{
            return delegate.getInt(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException{
        try{
            return delegate.getLong(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException{
        try{
            return delegate.getFloat(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException{
        try{
            return delegate.getDouble(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex,int scale) throws SQLException{
        try{
            return delegate.getBigDecimal(columnIndex,scale);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException{
        try{
            return delegate.getBytes(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException{
        try{
            return delegate.getDate(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException{
        try{
            return delegate.getTime(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException{
        try{
            return delegate.getTimestamp(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException{
        try{
            return delegate.getAsciiStream(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException{
        try{
            return delegate.getUnicodeStream(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException{
        try{
            return delegate.getBinaryStream(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException{
        try{
            return delegate.getString(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException{
        try{
            return delegate.getBoolean(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException{
        try{
            return delegate.getByte(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException{
        try{
            return delegate.getShort(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException{
        try{
            return delegate.getInt(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException{
        try{
            return delegate.getLong(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException{
        try{
            return delegate.getFloat(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException{
        try{
            return delegate.getDouble(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel,int scale) throws SQLException{
        try{
            return delegate.getBigDecimal(columnLabel,scale);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException{
        try{
            return delegate.getBytes(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException{
        try{
            return delegate.getDate(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException{
        try{
            return delegate.getTime(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException{
        try{
            return delegate.getTimestamp(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException{
        try{
            return delegate.getAsciiStream(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException{
        try{
            return delegate.getUnicodeStream(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException{
        try{
            return delegate.getBinaryStream(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException{
        try{
            return delegate.getWarnings();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void clearWarnings() throws SQLException{
        try{
            delegate.clearWarnings();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public String getCursorName() throws SQLException{
        try{
            return delegate.getCursorName();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException{
        try{
            return delegate.getMetaData();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException{
        try{
            return delegate.getObject(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException{
        try{
            return delegate.getObject(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException{
        try{
            return delegate.findColumn(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException{
        try{
            return delegate.getCharacterStream(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException{
        try{
            return delegate.getCharacterStream(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException{
        try{
            return delegate.getBigDecimal(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException{
        try{
            return delegate.getBigDecimal(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException{
        try{
            return delegate.isBeforeFirst();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean isAfterLast() throws SQLException{
        try{
            return delegate.isAfterLast();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean isFirst() throws SQLException{
        try{
            return delegate.isFirst();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean isLast() throws SQLException{
        try{
            return delegate.isLast();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void beforeFirst() throws SQLException{
        try{
            delegate.beforeFirst();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void afterLast() throws SQLException{
        try{
            delegate.afterLast();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean first() throws SQLException{
        try{
            return delegate.first();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean last() throws SQLException{
        try{
            return delegate.last();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getRow() throws SQLException{
        try{
            return delegate.getRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException{
        try{
            return delegate.absolute(row);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException{
        try{
            return delegate.relative(rows);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean previous() throws SQLException{
        try{
            return delegate.previous();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException{
        try{
            delegate.setFetchDirection(direction);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException{
        try{
            return delegate.getFetchDirection();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException{
        try{
            delegate.setFetchSize(rows);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getFetchSize() throws SQLException{
        try{
            return delegate.getFetchSize();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getType() throws SQLException{
        try{
            return delegate.getType();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getConcurrency() throws SQLException{
        try{
            return delegate.getConcurrency();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean rowUpdated() throws SQLException{
        try{
            return delegate.rowUpdated();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean rowInserted() throws SQLException{
        try{
            return delegate.rowInserted();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean rowDeleted() throws SQLException{
        try{
            return delegate.rowDeleted();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException{
        try{
            delegate.updateNull(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBoolean(int columnIndex,boolean x) throws SQLException{
        try{
            delegate.updateBoolean(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateByte(int columnIndex,byte x) throws SQLException{
        try{
            delegate.updateByte(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateShort(int columnIndex,short x) throws SQLException{
        try{
            delegate.updateShort(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateInt(int columnIndex,int x) throws SQLException{
        try{
            delegate.updateInt(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateLong(int columnIndex,long x) throws SQLException{
        try{
            delegate.updateLong(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateFloat(int columnIndex,float x) throws SQLException{
        try{
            delegate.updateFloat(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateDouble(int columnIndex,double x) throws SQLException{
        try{
            delegate.updateDouble(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBigDecimal(int columnIndex,BigDecimal x) throws SQLException{
        try{
            delegate.updateBigDecimal(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateString(int columnIndex,String x) throws SQLException{
        try{
            delegate.updateString(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBytes(int columnIndex,byte[] x) throws SQLException{
        try{
            delegate.updateBytes(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateDate(int columnIndex,Date x) throws SQLException{
        try{
            delegate.updateDate(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateTime(int columnIndex,Time x) throws SQLException{
        try{
            delegate.updateTime(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateTimestamp(int columnIndex,Timestamp x) throws SQLException{
        try{
            delegate.updateTimestamp(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex,InputStream x,int length) throws SQLException{
        try{
            delegate.updateAsciiStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex,InputStream x,int length) throws SQLException{
        try{
            delegate.updateBinaryStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex,Reader x,int length) throws SQLException{
        try{
            delegate.updateCharacterStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(int columnIndex,Object x,int scaleOrLength) throws SQLException{
        try{
            delegate.updateObject(columnIndex,x,scaleOrLength);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(int columnIndex,Object x) throws SQLException{
        try{
            delegate.updateObject(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException{
        try{
            delegate.updateNull(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBoolean(String columnLabel,boolean x) throws SQLException{
        try{
            delegate.updateBoolean(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateByte(String columnLabel,byte x) throws SQLException{
        try{
            delegate.updateByte(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateShort(String columnLabel,short x) throws SQLException{
        try{
            delegate.updateShort(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateInt(String columnLabel,int x) throws SQLException{
        try{
            delegate.updateInt(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateLong(String columnLabel,long x) throws SQLException{
        try{
            delegate.updateLong(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateFloat(String columnLabel,float x) throws SQLException{
        try{
            delegate.updateFloat(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateDouble(String columnLabel,double x) throws SQLException{
        try{
            delegate.updateDouble(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBigDecimal(String columnLabel,BigDecimal x) throws SQLException{
        try{
            delegate.updateBigDecimal(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateString(String columnLabel,String x) throws SQLException{
        try{
            delegate.updateString(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBytes(String columnLabel,byte[] x) throws SQLException{
        try{
            delegate.updateBytes(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateDate(String columnLabel,Date x) throws SQLException{
        try{
            delegate.updateDate(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateTime(String columnLabel,Time x) throws SQLException{
        try{
            delegate.updateTime(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateTimestamp(String columnLabel,Timestamp x) throws SQLException{
        try{
            delegate.updateTimestamp(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel,InputStream x,int length) throws SQLException{
        try{
            delegate.updateAsciiStream(columnLabel,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel,InputStream x,int length) throws SQLException{
        try{
            delegate.updateBinaryStream(columnLabel,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel,Reader reader,int length) throws SQLException{
        try{
            delegate.updateCharacterStream(columnLabel,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(String columnLabel,Object x,int scaleOrLength) throws SQLException{
        try{
            delegate.updateObject(columnLabel,x,scaleOrLength);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(String columnLabel,Object x) throws SQLException{
        try{
            delegate.updateObject(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void insertRow() throws SQLException{
        try{
            delegate.insertRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateRow() throws SQLException{
        try{
            delegate.updateRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void deleteRow() throws SQLException{
        try{
            delegate.deleteRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void refreshRow() throws SQLException{
        try{
            delegate.refreshRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void cancelRowUpdates() throws SQLException{
        try{
            delegate.cancelRowUpdates();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void moveToInsertRow() throws SQLException{
        try{
            delegate.moveToInsertRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void moveToCurrentRow() throws SQLException{
        try{
            delegate.moveToCurrentRow();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Statement getStatement() throws SQLException{
        return source;
    }

    @Override
    public Object getObject(int columnIndex,Map<String, Class<?>> map) throws SQLException{
        try{
            return delegate.getObject(columnIndex,map);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException{
        try{
            return delegate.getRef(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException{
        try{
            return delegate.getBlob(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException{
        try{
            return delegate.getClob(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException{
        try{
            return delegate.getArray(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Object getObject(String columnLabel,Map<String, Class<?>> map) throws SQLException{
        try{
            return delegate.getObject(columnLabel,map);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException{
        try{
            return delegate.getRef(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException{
        try{
            return delegate.getBlob(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException{
        try{
            return delegate.getClob(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException{
        try{
            return delegate.getArray(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Date getDate(int columnIndex,Calendar cal) throws SQLException{
        try{
            return delegate.getDate(columnIndex,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Date getDate(String columnLabel,Calendar cal) throws SQLException{
        try{
            return delegate.getDate(columnLabel,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Time getTime(int columnIndex,Calendar cal) throws SQLException{
        try{
            return delegate.getTime(columnIndex,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Time getTime(String columnLabel,Calendar cal) throws SQLException{
        try{
            return delegate.getTime(columnLabel,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex,Calendar cal) throws SQLException{
        try{
            return delegate.getTimestamp(columnIndex,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel,Calendar cal) throws SQLException{
        try{
            return delegate.getTimestamp(columnLabel,cal);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException{
        try{
            return delegate.getURL(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException{
        try{
            return delegate.getURL(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateRef(int columnIndex,Ref x) throws SQLException{
        try{
            delegate.updateRef(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateRef(String columnLabel,Ref x) throws SQLException{
        try{
            delegate.updateRef(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(int columnIndex,Blob x) throws SQLException{
        try{
            delegate.updateBlob(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(String columnLabel,Blob x) throws SQLException{
        try{
            delegate.updateBlob(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(int columnIndex,Clob x) throws SQLException{
        try{
            delegate.updateClob(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(String columnLabel,Clob x) throws SQLException{
        try{
            delegate.updateClob(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateArray(int columnIndex,Array x) throws SQLException{
        try{
            delegate.updateArray(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateArray(String columnLabel,Array x) throws SQLException{
        try{
            delegate.updateArray(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException{
        try{
            return delegate.getRowId(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException{
        try{
            return delegate.getRowId(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateRowId(int columnIndex,RowId x) throws SQLException{
        try{
            delegate.updateRowId(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateRowId(String columnLabel,RowId x) throws SQLException{
        try{
            delegate.updateRowId(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public int getHoldability() throws SQLException{
        try{
            return delegate.getHoldability();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException{
        try{
            return delegate.isClosed();
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNString(int columnIndex,String nString) throws SQLException{
        try{
            delegate.updateNString(columnIndex,nString);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNString(String columnLabel,String nString) throws SQLException{
        try{
            delegate.updateNString(columnLabel,nString);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(int columnIndex,NClob nClob) throws SQLException{
        try{
            delegate.updateNClob(columnIndex,nClob);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(String columnLabel,NClob nClob) throws SQLException{
        try{
            delegate.updateNClob(columnLabel,nClob);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException{
        try{
            return delegate.getNClob(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException{
        try{
            return delegate.getNClob(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException{
        try{
            return delegate.getSQLXML(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException{
        try{
            return delegate.getSQLXML(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateSQLXML(int columnIndex,SQLXML xmlObject) throws SQLException{
        try{
            delegate.updateSQLXML(columnIndex,xmlObject);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateSQLXML(String columnLabel,SQLXML xmlObject) throws SQLException{
        try{
            delegate.updateSQLXML(columnLabel,xmlObject);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException{
        try{
            return delegate.getNString(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public String getNString(String columnLabel) throws SQLException{
        try{
            return delegate.getNString(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException{
        try{
            return delegate.getNCharacterStream(columnIndex);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException{
        try{
            return delegate.getNCharacterStream(columnLabel);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex,Reader x,long length) throws SQLException{
        try{
            delegate.updateNCharacterStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{
        try{
            delegate.updateNCharacterStream(columnLabel,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex,InputStream x,long length) throws SQLException{
        try{
            delegate.updateAsciiStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex,InputStream x,long length) throws SQLException{
        try{
            delegate.updateBinaryStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex,Reader x,long length) throws SQLException{
        try{
            delegate.updateCharacterStream(columnIndex,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel,InputStream x,long length) throws SQLException{
        try{
            delegate.updateAsciiStream(columnLabel,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel,InputStream x,long length) throws SQLException{
        try{
            delegate.updateBinaryStream(columnLabel,x,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel,Reader reader,long length) throws SQLException{
        try{
            delegate.updateCharacterStream(columnLabel,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(int columnIndex,InputStream inputStream,long length) throws SQLException{
        try{
            delegate.updateBlob(columnIndex,inputStream,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(String columnLabel,InputStream inputStream,long length) throws SQLException{
        try{
            delegate.updateBlob(columnLabel,inputStream,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(int columnIndex,Reader reader,long length) throws SQLException{
        try{
            delegate.updateClob(columnIndex,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(String columnLabel,Reader reader,long length) throws SQLException{
        try{
            delegate.updateClob(columnLabel,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(int columnIndex,Reader reader,long length) throws SQLException{
        try{
            delegate.updateNClob(columnIndex,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(String columnLabel,Reader reader,long length) throws SQLException{
        try{
            delegate.updateNClob(columnLabel,reader,length);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNCharacterStream(int columnIndex,Reader x) throws SQLException{
        try{
            delegate.updateNCharacterStream(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNCharacterStream(String columnLabel,Reader reader) throws SQLException{
        try{
            delegate.updateNCharacterStream(columnLabel,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(int columnIndex,InputStream x) throws SQLException{
        try{
            delegate.updateAsciiStream(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(int columnIndex,InputStream x) throws SQLException{
        try{
            delegate.updateBinaryStream(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(int columnIndex,Reader x) throws SQLException{
        try{
            delegate.updateCharacterStream(columnIndex,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateAsciiStream(String columnLabel,InputStream x) throws SQLException{
        try{
            delegate.updateAsciiStream(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBinaryStream(String columnLabel,InputStream x) throws SQLException{
        try{
            delegate.updateBinaryStream(columnLabel,x);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateCharacterStream(String columnLabel,Reader reader) throws SQLException{
        try{
            delegate.updateCharacterStream(columnLabel,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(int columnIndex,InputStream inputStream) throws SQLException{
        try{
            delegate.updateBlob(columnIndex,inputStream);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateBlob(String columnLabel,InputStream inputStream) throws SQLException{
        try{
            delegate.updateBlob(columnLabel,inputStream);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(int columnIndex,Reader reader) throws SQLException{
        try{
            delegate.updateClob(columnIndex,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateClob(String columnLabel,Reader reader) throws SQLException{
        try{
            delegate.updateClob(columnLabel,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(int columnIndex,Reader reader) throws SQLException{
        try{
            delegate.updateNClob(columnIndex,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateNClob(String columnLabel,Reader reader) throws SQLException{
        try{
            delegate.updateNClob(columnLabel,reader);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public <T> T getObject(int columnIndex,Class<T> type) throws SQLException{
        try{
            return delegate.getObject(columnIndex,type);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public <T> T getObject(String columnLabel,Class<T> type) throws SQLException{
        try{
            return delegate.getObject(columnLabel,type);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(int columnIndex,Object x,SQLType targetSqlType,int scaleOrLength) throws SQLException{
        try{
            delegate.updateObject(columnIndex,x,targetSqlType,scaleOrLength);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(String columnLabel,Object x,SQLType targetSqlType,int scaleOrLength) throws SQLException{
        try{
            delegate.updateObject(columnLabel,x,targetSqlType,scaleOrLength);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(int columnIndex,Object x,SQLType targetSqlType) throws SQLException{
        try{
            delegate.updateObject(columnIndex,x,targetSqlType);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public void updateObject(String columnLabel,Object x,SQLType targetSqlType) throws SQLException{
        try{
            delegate.updateObject(columnLabel,x,targetSqlType);
        }catch(SQLException e){
            throw dealWithError(e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{
        throw new SQLFeatureNotSupportedException("Unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{
        return false;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private SQLException dealWithError(SQLException se) throws SQLException{
        //TODO -sf- implement
//        if(ClientErrors.willDisconnect(se))
        return se;
    }
}
