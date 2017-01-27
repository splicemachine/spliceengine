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

package com.splicemachine.db.impl.jdbc;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/* ---- New jdbc 2.0 types ----- */
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Ref;

import java.util.Map;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import java.util.Calendar;

import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;


/**
 * This class extends the EmbedCallableStatement class in order to support new
 * methods and classes that come with JDBC 2.0.
 *
 *	@see EmbedCallableStatement
 *
 */
public abstract class EmbedCallableStatement20
	extends EmbedCallableStatement
{

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	public EmbedCallableStatement20 (EmbedConnection conn, String sql,
								   int resultSetType,
								   int resultSetConcurrency,
								   int resultSetHoldability)
		throws SQLException
	{
		super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 2.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////


    /**
     * JDBC 2.0
     *
     * Returns an object representing the value of OUT parameter @i.
     * Use the @map to determine the class from which to construct 
     * data of SQL structured and distinct types.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param map the mapping from SQL type names to Java classes
     * @return a java.lang.Object holding the OUT parameter value.
     * @exception SQLException if a database-access error occurs.
     */
	public Object  getObject (int i, java.util.Map map) throws SQLException 
	{
		checkStatus();
		if( map == null)
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,map,"map",
                                              "java.sql.CallableStatement.getObject");
        if(!(map.isEmpty()))
            throw Util.notImplemented();
        // Map is empty call the normal getObject method.
        return getObject(i);
	}

    /**
     * JDBC 2.0
     *
     * Get a REF(&lt;structured-type&gt;) OUT parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @return an object representing data of an SQL REF Type
     * @exception SQLException if a database-access error occurs.
     */
	public Ref getRef (int i) throws SQLException {
		throw Util.notImplemented();
	}

    /**
     * JDBC 2.0
     *
     * Get an Array OUT parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @return an object representing an SQL array
     * @exception SQLException if a database-access error occurs.
     */
    public Array getArray (int i) throws SQLException {
		throw Util.notImplemented();
	}


 
	/*
	 * Note: all the JDBC 2.0 Prepared statement methods are duplicated in here
	 * because this class inherits from Local/EmbedCallableStatement, which
	 * inherits from local/PreparedStatement.  This class should inherit from a
	 * local20/PreparedStatement.  Since java does not allow multiple inheritance,
	 * duplicate the code here.
	 */
 
     /**
      * JDBC 2.0
      *
      * Set a REF(&lt;structured-type&gt;) parameter.
      *
      * @param i the first parameter is 1, the second is 2, ...
      * @param x an object representing data of an SQL REF Type
      * @exception SQLException Feature not implemented for now.
      */
     public void setRef (int i, Ref x) throws SQLException {
 		throw Util.notImplemented();
	 }
 
     /**
      * JDBC 2.0
      *
      * Set an Array parameter.
      *
      * @param i the first parameter is 1, the second is 2, ...
      * @param x an object representing an SQL array
      * @exception SQLException Feature not implemented for now.
      */
     public void setArray (int i, Array x) throws SQLException {
 		throw Util.notImplemented();
	 }
 

	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 3.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

	/**
    * JDBC 3.0
    *
    * Registers the OUT parameter named parameterName to the JDBC type sqlType.
    * All OUT parameters must be registered before a stored procedure is executed.
    *
    * @param parameterName - the name of the parameter
    * @param sqlType - the JDBC type code defined by java.sql.Types. If the
    * parameter is of JDBC type NUMERIC or DECIMAL, the version of registerOutParameter
    * that accepts a scale value should be used.
    * @exception SQLException Feature not implemented for now.
	*/
	public void registerOutParameter(String parameterName,
					int sqlType)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Registers the designated output parameter. This version of the method
    * registerOutParameter should be used for a user-named or REF output parameter.
    *
    * @param parameterName - the name of the parameter
    * @param sqlType - the SQL type code defined by java.sql.Types.
    * @param typeName - the fully-qualified name of an SQL structure type
    * @exception SQLException Feature not implemented for now.
	*/
	public void registerOutParameter(String parameterName,
					int sqlType, String typeName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Registers the parameter named parameterName to the JDBC type sqlType.
    * This method must be called before a stored procedure is executed.
    *
    * @param parameterName - the name of the parameter
    * @param sqlType - the SQL type code defined by java.sql.Types.
    * @param scale - the desired number of digits to the right of the decimal point.
    * It must be greater than or equal to zero.
    * @exception SQLException Feature not implemented for now.
	*/
	public void registerOutParameter(String parameterName,
					int sqlType, int scale)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC REF (<structured-type) parameter as a Ref object
    * in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value as a Ref object in the Java Programming language.
    * If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Ref getRef(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC BLOB parameter as a Blob object
    * in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value as a Blob object in the Java Programming language.
    * If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Blob getBlob(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC CLOB parameter as a Clob object
    * in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value as a Clob object in the Java Programming language.
    * If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Clob getClob(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC ARRAY parameter as an Array object
    * in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value as a Array object in the Java Programming language.
    * If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Array getArray(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to SQL NULL.
    *
    * @param parameterName - the name of the parameter
    * @param sqlType - the SQL type code defined in java.sql.Types
    * @exception SQLException Feature not implemented for now.
	*/
	public void setNull(String parameterName, int sqlType)
    throws SQLException
	{
		throw Util.notImplemented();
	}
	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to SQL NULL.
    *
    * @param parameterName - the name of the parameter
    * @param sqlType - the SQL type code defined in java.sql.Types
    * @param typeName - the fully-qualified name of an SQL user-defined type
    * @exception SQLException Feature not implemented for now.
	*/
	public void setNull(String parameterName, int sqlType, String typeName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java boolean value. The driver
    * converts this to an SQL BIT value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setBoolean(String parameterName, boolean x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC BIT parameter as a boolean in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is false.
    * @exception SQLException Feature not implemented for now.
	*/
	public boolean getBoolean(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java byte value. The driver
    * converts this to an SQL TINYINT value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setByte(String parameterName, byte x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC TINYINT parameter as a byte in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public byte getByte(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java short value. The driver
    * converts this to an SQL SMALLINT value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setShort(String parameterName, short x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC SMALLINT parameter as a short in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public short getShort(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java int value. The driver
    * converts this to an SQL INTEGER value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setInt(String parameterName, int x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC INTEGER parameter as a int in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public int getInt(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java long value. The driver
    * converts this to an SQL BIGINT value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setLong(String parameterName, long x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC BIGINT parameter as a long in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public long getLong(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java float value. The driver
    * converts this to an SQL FLOAT value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setFloat(String parameterName, float x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC FLOAT parameter as a float in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public float getFloat(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java double value. The driver
    * converts this to an SQL DOUBLE value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setDouble(String parameterName, double x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC DOUBLE parameter as a double in the Java
    * programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public double getDouble(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.math.BigDecimal value. The driver
    * converts this to an SQL NUMERIC value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setBigDecimal(String parameterName, BigDecimal x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC NUMERIC parameter as a java.math.BigDecimal
    * object with as many digits to the right of the decimal point as the value contains
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is 0.
    * @exception SQLException Feature not implemented for now.
	*/
	public BigDecimal getBigDecimal(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java String value. The driver
    * converts this to an SQL VARCHAR OR LONGVARCHAR value (depending on the
    * argument's size relative the driver's limits on VARCHAR values) when it
    * sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setString(String parameterName, String x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC CHAR, VARCHAR, or LONGVARCHAR parameter as
    * a String in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public String getString(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Java array of bytes. The driver
    * converts this to an SQL VARBINARY OR LONGVARBINARY (depending on the argument's
    * size relative to the driver's limits on VARBINARY values)when it sends it to
    * the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setBytes(String parameterName, byte[] x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC BINARY or VARBINARY parameter as an array
    * of byte values in the Java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public byte[] getBytes(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Date value. The driver
    * converts this to an SQL DATE value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setDate(String parameterName, Date x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Date value, using the given
    * Calendar object.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @param cal - the Calendar object the driver will use to construct the date
    * @exception SQLException Feature not implemented for now.
	*/
	public void setDate(String parameterName, Date x, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC DATE parameter as ajava.sql.Date object
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Date getDate(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC DATE parameter as a java.sql.Date object,
    * using the given Calendar object to construct the date object.
    *
    * @param parameterName - the name of the parameter
    * @param cal - the Calendar object the driver will use to construct the date
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Date getDate(String parameterName, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Time value. The driver
    * converts this to an SQL TIME value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setTime(String parameterName, Time x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC TIME parameter as ajava.sql.Time object
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Time getTime(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC TIME parameter as a java.sql.Time object,
    * using the given Calendar object to construct the time object.
    *
    * @param parameterName - the name of the parameter
    * @param cal - the Calendar object the driver will use to construct the time
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Time getTime(String parameterName, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Time value using the
    * Calendar object
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @param cal - the Calendar object the driver will use to construct the time
    * @exception SQLException Feature not implemented for now.
	*/
	public void setTime(String parameterName, Time x, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Timestamp value. The driver
    * converts this to an SQL TIMESTAMP value when it sends it to the database.
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setTimestamp(String parameterName, Timestamp x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given java.sql.Timestamp value, using the
    * given Calendar object
    *
    * @param parameterName - the name of the parameter
    * @param x - the parameter value
    * @param cal - the Calendar object the driver will use to construct the timestamp.
    * @exception SQLException Feature not implemented for now.
	*/
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp object
    *
    * @param parameterName - the name of the parameter
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Timestamp getTimestamp(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a JDBC TIMESTAMP parameter as a java.sql.Timestamp object,
    * using the given Calendar object to construct the Timestamp object.
    *
    * @param parameterName - the name of the parameter
    * @param cal - the Calendar object the driver will use to construct the Timestamp
    * @return the parameter value. If the value is SQL NULL, the result is null.
    * @exception SQLException Feature not implemented for now.
	*/
	public Timestamp getTimestamp(String parameterName, Calendar cal)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given input stream, which will have the
    * specified number of bytes.
    *
    * @param parameterName - the name of the parameter
    * @param x - the Java input stream that contains the ASCII parameter value
    * @param length - the number of bytes in the stream
    * @exception SQLException Feature not implemented for now.
	*/
	public void setAsciiStream(String parameterName, InputStream x, int length)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given input stream, which will have the
    * specified number of bytes.
    *
    * @param parameterName - the name of the parameter
    * @param x - the Java input stream that contains the binary parameter value
    * @param length - the number of bytes in the stream
    * @exception SQLException Feature not implemented for now.
	*/
	public void setBinaryStream(String parameterName, InputStream x, int length)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the designated parameter to the given Reader object, which is the given
    * number of characters long.
    *
    * @param parameterName - the name of the parameter
    * @param reader - the java.io.Reader object that contains the UNICODE data
    * @param length - the number of characters in the stream
    * @exception SQLException Feature not implemented for now.
	*/
	public void setCharacterStream(String parameterName, Reader reader, int length)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the value of the designated parameter with the given object. The second
    * argument must be an object type; for integral values, the java.lang equivalent
    * objects should be used.
    *
    * @param parameterName - the name of the parameter
    * @param x - the object containing the input parameter value
    * @param targetSqlType - the SQL type (as defined in java.sql.Types) to be sent to
    * the database. The scale argument may further qualify this type.
    * @param scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types, this
    * is the number of digits after the decimal point. For all other types, this value
    * will be ignored.
    * @exception SQLException Feature not implemented for now.
	*/
	public void setObject(String parameterName, Object x, int targetSqlType, int scale)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Retrieves the value of a parameter as an Object in the java programming language.
    *
    * @param parameterName - the name of the parameter
    * @return a java.lang.Object holding the OUT parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public Object getObject(String parameterName)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Returns an object representing the value of OUT parameter i and uses map for
    * the custom mapping of the parameter value.
    *
    * @param parameterName - the name of the parameter
    * @param map - the mapping from SQL type names to Java classes
    * @return a java.lang.Object holding the OUT parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public Object getObject(String parameterName, Map map)
    throws SQLException
	{
		checkStatus();
		if( map == null)
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,map,"map",
                                              "java.sql.CallableStatement.getObject");
        if(!(map.isEmpty()))
            throw Util.notImplemented();

        // Map is empty so call the normal getObject method.
        return getObject(parameterName);
	}

	/**
    * JDBC 3.0
    *
    * Sets the value of the designated parameter with the given object. This method
    * is like the method setObject above, except that it assumes a scale of zero.
    *
    * @param parameterName - the name of the parameter
    * @param x - the object containing the input parameter value
    * @param targetSqlType - the SQL type (as defined in java.sql.Types) to be sent to
    * the database. 
    * @exception SQLException Feature not implemented for now.
	*/
	public void setObject(String parameterName, Object x, int targetSqlType)
    throws SQLException
	{
		throw Util.notImplemented();
	}

	/**
    * JDBC 3.0
    *
    * Sets the value of the designated parameter with the given object. The second
    * parameter must be of type Object; therefore, the java.lang equivalent objects
    * should be used for built-in types.
    *
    * @param parameterName - the name of the parameter
    * @param x - the object containing the input parameter value
    * @exception SQLException Feature not implemented for now.
	*/
	public void setObject(String parameterName, Object x)
    throws SQLException
	{
		throw Util.notImplemented();
	}

    /////////////////////////////////////////////////////////////////////////
    //
    //	JDBC 4.0	-	New public methods
    //
    /////////////////////////////////////////////////////////////////////////
    
    /**
     * Retrieves the value of the designated parameter as a 
     * <code>java.io.Reader</code> object in the Java programming language.
     * Introduced in JDBC 4.0.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the parameter
     *         value; if the value is SQL <code>NULL</code>, the value returned
     *         is <code>null</code> in the Java programming language.
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed <code>CallableStatement</code>
     */
    public Reader getCharacterStream(int parameterIndex)
        throws SQLException {
        checkStatus();
        // Make sure the specified parameter has mode OUT or IN/OUT.
        switch (getParms().getParameterMode(parameterIndex)) {
            case JDBC30Translation.PARAMETER_MODE_IN:
            case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
                throw newSQLException(SQLState.LANG_NOT_OUTPUT_PARAMETER,
                                      Integer.toString(parameterIndex));
        }
        Reader reader = null;
        int paramType = getParameterJDBCType(parameterIndex);
        switch (paramType) {
            // Handle character/string types.
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                boolean pushStack = false;
                Object syncObject = getConnectionSynchronization();
                synchronized (syncObject) {
                try {
                    StringDataValue param = (StringDataValue)
                        getParms().getParameterForGet(parameterIndex -1);
                    if (param.isNull()) {
                        break;
                    }
                    pushStack = true;
                    setupContextStack();

                    if (param.hasStream()) {
                        CharacterStreamDescriptor csd =
                            param.getStreamWithDescriptor();
                        reader = new UTF8Reader(csd, this, syncObject);
                    } else {
                        reader = new StringReader(param.getString());
                    }
                } catch (Throwable t) {
                    throw EmbedResultSet.noStateChangeException(t);
                } finally {
                    if (pushStack) {
                        restoreContextStack();
                    }
                }
                } // End synchronized block
                break;

            // Handle binary types.
            // JDBC says to support these, but no defintion exists for the output.
            // Match JCC which treats the bytes as a UTF-16BE stream.
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                try {
                    InputStream is = getBinaryStream(parameterIndex);
                    if (is != null) {
                        reader = new InputStreamReader(is, "UTF-16BE");
                    }
                    break;
                } catch (UnsupportedEncodingException uee) {
                    throw newSQLException(uee.getMessage());
                }

            default:
                throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, 
                        "java.io.Reader", Util.typeName(paramType));
        } 
        // Update wasNull. 
        wasNull = (reader == null);
        return reader;
    }
    
    // Private utility classes

    /**
     * Get binary stream for a parameter.
     *
     * @param parameterIndex first parameter is 1, second is 2 etc.
     * @return a stream for the binary parameter, or <code>null</code>.
     *
     * @throws SQLException if a database access error occurs.
     */
    private InputStream getBinaryStream(int parameterIndex)
        throws SQLException {
        int paramType = getParameterJDBCType(parameterIndex); 
        switch (paramType) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                break;
            default:
                throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH, 
                        "java.io.InputStream", Util.typeName(paramType));
        }

        boolean pushStack = false;
        synchronized (getConnectionSynchronization()) {
            try {
                DataValueDescriptor param = 
                    getParms().getParameterForGet(parameterIndex -1);
                wasNull = param.isNull();
                if (wasNull) {
                    return null;
                }
                pushStack = true;
                setupContextStack();

                InputStream stream; // The stream we will return to the user
                if (param.hasStream()) {
                    stream = new BinaryToRawStream(param.getStream(), param);
                } else {
                    stream = new ByteArrayInputStream(param.getBytes());
                }
                return stream;
            } catch (Throwable t) {
                throw EmbedResultSet.noStateChangeException(t);
            } finally {
                if (pushStack) {
                    restoreContextStack();
                }
            }
        } // End synchronized block
    }
}
