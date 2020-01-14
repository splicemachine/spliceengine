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
import java.util.Calendar;

import java.sql.*;

/**
	JDBC 2 brokered PreparedStatement. Forwards calls off to a real prepared statement
	obtained through the BrokeredStatementControl getRealPreparedStatement method.
 */
public abstract class BrokeredPreparedStatement extends BrokeredStatement
	implements EnginePreparedStatement
{

	/**
		SQL used to create me.
	*/
	final String	sql;

    public BrokeredPreparedStatement(BrokeredStatementControl control, String sql) throws SQLException
    {
        super(control);
		this.sql = sql;
    }

	/**
     * A prepared SQL query is executed and its ResultSet is returned.
     *
     * @return a ResultSet that contains the data produced by the
     * query; never null
	 * @exception SQLException thrown on failure.
     */
	public final ResultSet executeQuery() throws SQLException
    {
        return wrapResultSet(getPreparedStatement().executeQuery());
    } 

    /**
     * Execute a SQL INSERT, UPDATE or DELETE statement. In addition,
     * SQL statements that return nothing such as SQL DDL statements
     * can be executed.
     *
     * @return either the row count for INSERT, UPDATE or DELETE; or 0
     * for SQL statements that return nothing
	 * @exception SQLException thrown on failure.
     */
	public final int executeUpdate() throws SQLException
    {
        return getPreparedStatement().executeUpdate();
    }

	public void close() throws SQLException
	{
	    control.closeRealPreparedStatement();
	}
	
    /**
     * Set a parameter to SQL NULL.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType SQL type code defined by java.sql.Types
	 * @exception SQLException thrown on failure.
     */
    public final void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        getPreparedStatement().setNull( parameterIndex, sqlType);
    } 

    /**
     * Set a parameter to SQL NULL.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType SQL type code defined by java.sql.Types
	 * @exception SQLException thrown on failure.
     */
    public final void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        getPreparedStatement().setNull( parameterIndex, sqlType, typeName);
    } 

    /**
     * Set a parameter to a Java boolean value.  According to the JDBC API spec,
	 * the driver converts this to a SQL BIT value when it sends it to the
	 * database. But we don't have to do this, since the database engine
	 * supports a boolean type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        getPreparedStatement().setBoolean( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java byte value.  The driver converts this
     * to a SQL TINYINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setByte(int parameterIndex, byte x) throws SQLException
    {
        getPreparedStatement().setByte( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java short value.  The driver converts this
     * to a SQL SMALLINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setShort(int parameterIndex, short x) throws SQLException
    {
        getPreparedStatement().setShort( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java int value.  The driver converts this
     * to a SQL INTEGER value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setInt(int parameterIndex, int x) throws SQLException
    {
        getPreparedStatement().setInt( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java long value.  The driver converts this
     * to a SQL BIGINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setLong(int parameterIndex, long x) throws SQLException
    {
        getPreparedStatement().setLong( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java float value.  The driver converts this
     * to a SQL FLOAT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setFloat(int parameterIndex, float x) throws SQLException
    {
        getPreparedStatement().setFloat( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java double value.  The driver converts this
     * to a SQL DOUBLE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setDouble(int parameterIndex, double x) throws SQLException
    {
        getPreparedStatement().setDouble( parameterIndex, x);
    } 


    /**
     * Set a parameter to a java.math.BigDecimal value.  
     * The driver converts this to a SQL NUMERIC value when
     * it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setBigDecimal(int parameterIndex, java.math.BigDecimal x) throws SQLException
    {
        getPreparedStatement().setBigDecimal( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java String value.  The driver converts this
     * to a SQL VARCHAR or LONGVARCHAR value (depending on the arguments
     * size relative to the driver's limits on VARCHARs) when it sends
     * it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setString(int parameterIndex, String x) throws SQLException
    {
        getPreparedStatement().setString( parameterIndex, x);
    } 

    /**
     * Set a parameter to a Java array of bytes.  The driver converts
     * this to a SQL VARBINARY or LONGVARBINARY (depending on the
     * argument's size relative to the driver's limits on VARBINARYs)
     * when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value 
	 * @exception SQLException thrown on failure.
     */
    public final void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        getPreparedStatement().setBytes( parameterIndex, x);
    } 

    /**
     * Set a parameter to a java.sql.Date value.  The driver converts this
     * to a SQL DATE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setDate(int parameterIndex, Date x) throws SQLException
    {
        getPreparedStatement().setDate( parameterIndex, x);
    } 

    /**
     * Set a parameter to a java.sql.Time value.  The driver converts this
     * to a SQL TIME value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
	 * @exception SQLException thrown on failure.
     */
    public final void setTime(int parameterIndex, Time x) throws SQLException
    {
        getPreparedStatement().setTime( parameterIndex, x);
    } 

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver
     * converts this to a SQL TIMESTAMP value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value 
	 * @exception SQLException thrown on failure.
     */
    public final void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        getPreparedStatement().setTimestamp( parameterIndex, x);
    } 

    /**
	 * We do this inefficiently and read it all in here. The target type
	 * is assumed to be a String.
     * 
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the ASCII parameter value
     * @param length the number of bytes in the stream 
	 * @exception SQLException thrown on failure.
     */
    public final void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        getPreparedStatement().setAsciiStream( parameterIndex, x, length);
    } 

    /**
	 * We do this inefficiently and read it all in here. The target type
	 * is assumed to be a String. The unicode source is assumed to be
	 * in char[].  RESOLVE: might it be in UTF, instead? that'd be faster!
     * 
     * @param parameterIndex the first parameter is 1, the second is 2, ...  
     * @param x the java input stream which contains the
     * UNICODE parameter value 
     * @param length the number of bytes in the stream 
	 * @exception SQLException thrown on failure.
     * @deprecated
     */
    public final void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        getPreparedStatement().setUnicodeStream( parameterIndex, x, length);
    } 

    /**
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream 
	 * @exception SQLException thrown on failure.
     */
    public final void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        getPreparedStatement().setBinaryStream( parameterIndex, x, length);
    } 

    /**
     * JDBC 2.0
     *
     * Add a set of parameters to the batch.
     * 
     * @exception SQLException if a database-access error occurs.
     */
    public final void addBatch() throws SQLException
    {
        getPreparedStatement().addBatch( );
    } 

    /**
     * <P>In general, parameter values remain in force for repeated use of a
     * Statement. Setting a parameter value automatically clears its
     * previous value.  However, in some cases it is useful to immediately
     * release the resources used by the current parameter values; this can
     * be done by calling clearParameters.
	 * @exception SQLException thrown on failure.
     */
    public final void clearParameters() throws SQLException
    {
        getPreparedStatement().clearParameters( );
    } 

    /**
	 * JDBC 2.0
	 *
     * The number, types and properties of a ResultSet's columns
     * are provided by the getMetaData method.
     *
     * @return the description of a ResultSet's columns
     * @exception SQLException Feature not implemented for now.
     */
	public final java.sql.ResultSetMetaData getMetaData() throws SQLException
    {
        return getPreparedStatement().getMetaData();
    }

    /**
	 * The interface says that the type of the Object parameter must
	 * be compatible with the type of the targetSqlType. We check that,
	 * and if it flies, we expect the underlying engine to do the
	 * required conversion once we pass in the value using its type.
	 * So, an Integer converting to a CHAR is done via setInteger()
	 * support on the underlying CHAR type.
     *
     * <p>If x is null, it won't tell us its type, so we pass it on to setNull
     *
     * @param parameterIndex The first parameter is 1, the second is 2, ...
     * @param x The object containing the input parameter value
     * @param targetSqlType The SQL type (as defined in java.sql.Types) to be 
     * sent to the database. The scale argument may further qualify this type.
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *          this is the number of digits after the decimal.  For all other
     *          types this value will be ignored,
	 * @exception SQLException thrown on failure.
     */
    public final void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
        throws SQLException
    {
        getPreparedStatement().setObject( parameterIndex, x, targetSqlType, scale);
    } 
        
    /**
      * This method is like setObject above, but assumes a scale of zero.
      * @exception SQLException thrown on failure.
      */
    public final void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException
    {
        getPreparedStatement().setObject( parameterIndex, x, targetSqlType);
    } 

    /**
     * <p>Set the value of a parameter using an object; use the
     * java.lang equivalent objects for integral values.
     *
     * <p>The JDBC specification specifies a standard mapping from
     * Java Object types to SQL types.  The given argument java object
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     *
     * <p>Note that this method may be used to pass datatabase
     * specific abstract data types, by using a Driver specific Java
     * type.
     *
     * @param parameterIndex The first parameter is 1, the second is 2, ...
     * @param x The object containing the input parameter value 
	 * @exception SQLException thrown on failure.
     */
    public final void setObject(int parameterIndex, Object x)
        throws SQLException
    {
        getPreparedStatement().setObject( parameterIndex, x);
    } 

    /**
     * @see java.sql.Statement#execute
	 * @exception SQLException thrown on failure.
     */
    public final boolean execute() throws SQLException
    {
        return getPreparedStatement().execute();
    }

    public final void setCharacterStream(int parameterIndex,
                                   Reader reader,
                                   int length)
        throws SQLException
    {
        getPreparedStatement().setCharacterStream( parameterIndex, reader, length);
    }

    public final void setRef(int i,
                       Ref x)
        throws SQLException
    {
        getPreparedStatement().setRef( i, x);
    }

    public final void setBlob(int i,
                       Blob x)
        throws SQLException
    {
        getPreparedStatement().setBlob( i, x);
    }

    public final void setClob(int i,
                       Clob x)
        throws SQLException
    {
        getPreparedStatement().setClob( i, x);
    }

    public final void setArray(int i,
                         Array x)
        throws SQLException
    {
        getPreparedStatement().setArray( i, x);
    }

    public final void setDate(int i,
                        Date x,
                        Calendar cal)
        throws SQLException
    {
        getPreparedStatement().setDate( i, x, cal);
    }

    public final void setTime(int i,
                        Time x,
                        Calendar cal)
        throws SQLException
    {
        getPreparedStatement().setTime( i, x, cal);
    }

    public final void setTimestamp(int i,
                             Timestamp x,
                             Calendar cal)
        throws SQLException
    {
        getPreparedStatement().setTimestamp( i, x, cal);
    }
    
    
    public void setBinaryStream(int parameterIndex, 
                                InputStream x)
        throws SQLException {
        
        final EnginePreparedStatement engnps = 
            (EnginePreparedStatement) getPreparedStatement();  

        engnps.setBinaryStream(parameterIndex, x);

    }
    
    
    public void setCharacterStream(int parameterIndex, 
                                   Reader reader)
        throws SQLException{
        
        final EnginePreparedStatement engnps = 
            (EnginePreparedStatement) getPreparedStatement();  

        engnps.setCharacterStream(parameterIndex, reader);
        
    }

	/*
	** Control methods.
	*/

    /**
     * Access the underlying PreparedStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    PreparedStatement getPreparedStatement() throws SQLException {
		return control.getRealPreparedStatement();
	}

	/**
		Override the BrokeredStatement's getStatement() to always return a PreparedStatement.
	*/
	public final Statement getStatement() throws SQLException {
		return getPreparedStatement();
	}

	/**
		Create a duplicate PreparedStatement to this, including state, from the passed in Connection.
	*/
	public PreparedStatement createDuplicateStatement(Connection conn, PreparedStatement oldStatement) throws SQLException {

		PreparedStatement newStatement = conn.prepareStatement(sql, resultSetType, resultSetConcurrency);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}

    public final long getVersionCounter() throws SQLException {
        return ((EnginePreparedStatement)getPreparedStatement()).
                getVersionCounter();
    }

    @Override
    public void setURL(int parameterIndex,URL x) throws SQLException{
        getPreparedStatement().setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException{
        return getPreparedStatement().getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex,RowId x) throws SQLException{
        getPreparedStatement().setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex,String value) throws SQLException{
        getPreparedStatement().setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex,Reader value,long length) throws SQLException{
        getPreparedStatement().setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex,NClob value) throws SQLException{
        getPreparedStatement().setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex,Reader reader,long length) throws SQLException{
        getPreparedStatement().setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream,long length) throws SQLException{
        getPreparedStatement().setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex,Reader reader,long length) throws SQLException{
        getPreparedStatement().setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex,SQLXML xmlObject) throws SQLException{
        getPreparedStatement().setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x,long length) throws SQLException{
        getPreparedStatement().setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex,InputStream x,long length) throws SQLException{
        getPreparedStatement().setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex,Reader reader,long length) throws SQLException{
        getPreparedStatement().setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex,InputStream x) throws SQLException{
        getPreparedStatement().setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex,Reader value) throws SQLException{
        getPreparedStatement().setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex,Reader reader) throws SQLException{
        getPreparedStatement().setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex,InputStream inputStream) throws SQLException{
        getPreparedStatement().setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex,Reader reader) throws SQLException{
        getPreparedStatement().setNClob(parameterIndex, reader);
    }
}
