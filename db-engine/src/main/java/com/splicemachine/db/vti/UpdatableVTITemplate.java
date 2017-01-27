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

package com.splicemachine.db.vti;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;


/**

   An abstract implementation of PreparedStatement (JDBC 3.0) that is useful
	when writing a read-write (updatable) virtual table interface (VTI).
	
	This class implements
	the methods of the JDBC3.0 version of PreparedStatement
    each one throwing a SQLException
	with the name of the method. A concrete subclass can then just implement
	the methods not implemented here and override any methods it needs
	to implement for correct functionality.

 */
public abstract class UpdatableVTITemplate implements PreparedStatement 
{
	protected UpdatableVTITemplate() {}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet executeQuery(String sql) throws SQLException
	{
        throw new SQLException("executeQuery");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int executeUpdate(String sql) throws SQLException
	{
        throw new SQLException("executeUpdate");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void close() throws SQLException
	{
        throw new SQLException("close");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public SQLWarning getWarnings() throws SQLException
	{
        throw new SQLException("getWarnings");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearWarnings() throws SQLException
	{
        throw new SQLException("clearWarnings");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getMaxFieldSize() throws SQLException
	{
        throw new SQLException("getMaxFieldSize");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setMaxFieldSize(int max) throws SQLException
	{
        throw new SQLException("setMaxFieldSize");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getMaxRows() throws SQLException
	{
        throw new SQLException("getMaxRows");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setMaxRows(int max) throws SQLException
	{
        throw new SQLException("setMaxRows");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
        throw new SQLException("setEscapeProcessing");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getQueryTimeout() throws SQLException
	{
        throw new SQLException("getQueryTimeout");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setQueryTimeout(int seconds) throws SQLException
	{
        throw new SQLException("setQueryTimeout");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void addBatch(String sql) throws SQLException
	{
        throw new SQLException("addBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearBatch() throws SQLException
	{
        throw new SQLException("clearBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int[] executeBatch() throws SQLException
	{
        throw new SQLException("executeBatch");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void cancel() throws SQLException
	{
        throw new SQLException("cancel");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setCursorName(String name) throws SQLException
	{
        throw new SQLException("setCursorName");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean execute(String sql) throws SQLException
	{
        throw new SQLException("execute");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet getResultSet() throws SQLException
	{
        throw new SQLException("getResultSet");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getUpdateCount() throws SQLException
	{
        throw new SQLException("getUpdateCount");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean getMoreResults() throws SQLException
	{
        throw new SQLException("getMoreResults");
	}

	/**
	 * @see java.sql.Statement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int getResultSetConcurrency() throws SQLException
	{
        return ResultSet.CONCUR_UPDATABLE;
	}

    //
    // java.sql.PreparedStatement calls, passed through to our preparedStatement.
    //

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSet executeQuery() throws SQLException
	{
        throw new SQLException("executeQuery");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public int executeUpdate() throws SQLException
	{
        throw new SQLException("executeUpdate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setNull(int parameterIndex, int jdbcType) throws SQLException
	{
        throw new SQLException("setNull");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setNull(int parameterIndex, int jdbcType, String typeName) throws SQLException
	{
        throw new SQLException("setNull");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
        throw new SQLException("setBoolean");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
        throw new SQLException("setByte");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setShort(int parameterIndex, short x) throws SQLException
	{
        throw new SQLException("setShort");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setInt(int parameterIndex, int x) throws SQLException
	{
        throw new SQLException("setInt");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setLong(int parameterIndex, long x) throws SQLException
	{
        throw new SQLException("setLong");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
        throw new SQLException("setFloat");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
        throw new SQLException("setDouble");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
        throw new SQLException("setBigDecimal");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setString(int parameterIndex, String x) throws SQLException
	{
        throw new SQLException("setString");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBytes(int parameterIndex, byte x[]) throws SQLException
	{
        throw new SQLException("setBytes");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
        throw new SQLException("setDate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
        throw new SQLException("setTime");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
        throw new SQLException("setTimestamp");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setAsciiStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
     * @deprecated
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setUnicodeStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
        throw new SQLException("setBinaryStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void clearParameters() throws SQLException
	{
        throw new SQLException("clearParameters");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x, int targetJdbcType, int scale) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x, int targetJdbcType) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
        throw new SQLException("setObject");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public boolean execute() throws SQLException
	{
        throw new SQLException("execute");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
 	 * @exception SQLException on unexpected JDBC error
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
        throw new SQLException("ResultSetMetaData");
	}
	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getResultSetType() throws SQLException {
		throw new SQLException("getResultSetType");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setBlob(int i, Blob x) throws SQLException {
		throw new SQLException("setBlob");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("setFetchDirection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException("setFetchSize");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void addBatch() throws SQLException {
		throw new SQLException("addBatch");
	}


	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setCharacterStream(int parameterIndex,
									Reader reader,
									int length) throws SQLException {
		throw new SQLException("setCharacterStream");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public Connection getConnection() throws SQLException {
		throw new SQLException("getConnection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getFetchDirection() throws SQLException {
		throw new SQLException("getFetchDirection");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
								throws SQLException {
		throw new SQLException("setTime");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
								throws SQLException {
		throw new SQLException("setTimestamp");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public int getFetchSize() throws SQLException {
		throw new SQLException("getFetchSize");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setRef(int i, Ref x) throws SQLException {
		throw new SQLException("setRef");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
						throws SQLException {
		throw new SQLException("setDate");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setClob(int i, Clob x) throws SQLException {
		throw new SQLException("setClob");
	}

	/**
	 * @see java.sql.PreparedStatement
	 *
	 * @exception SQLException		Always thrown
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw new SQLException("setArray");
	}

    public void setURL(int arg0, URL arg1) throws SQLException {
        throw new SQLException("setURL");    
    }

    public boolean getMoreResults(int arg0) throws SQLException {
        throw new SQLException("getMoreResults");
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("getGeneratedKeys");
    }

    public int executeUpdate(String arg0, int arg1) throws SQLException {
        throw new SQLException("executeUpdate");
    }

    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("executeUpdate");
    }

    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("executeUpdate");
    }

    public boolean execute(String arg0, int arg1) throws SQLException {
        throw new SQLException("execute");
    }

    public boolean execute(String arg0, int[] arg1) throws SQLException {
        throw new SQLException("execute");
    }

    public boolean execute(String arg0, String[] arg1) throws SQLException {
        throw new SQLException("execute");
    }

    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("getResultSetHoldability");
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("getParameterMetaData");
    }
}
