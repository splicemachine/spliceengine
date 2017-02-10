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

import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.SQLState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * Local implementation.
 *
 */
public abstract class EmbedCallableStatement extends EmbedPreparedStatement
	implements CallableStatement
{
	/*
	** True if we are of the form ? = CALL() -- i.e. true
	** if we have a return output parameter.
	*/
	private boolean hasReturnOutputParameter;

	protected boolean	wasNull;

	/**
	 * @exception SQLException thrown on failure
	 */
	public EmbedCallableStatement (EmbedConnection conn, String sql,
								   int resultSetType,
								   int resultSetConcurrency,
								   int resultSetHoldability)
		throws SQLException
	{
	    super(conn, sql, false, 
			  resultSetType,
			  resultSetConcurrency,
			  resultSetHoldability,
			  Statement.NO_GENERATED_KEYS,
			  null,
			  null);

		// mark our parameters as for a callable statement 
		ParameterValueSet pvs = getParms();

		// do we have a return parameter?
		hasReturnOutputParameter = pvs.hasReturnOutputParameter();
	}

	protected void checkRequiresCallableStatement(Activation activation) {
	}

	protected final boolean executeStatement(Activation a,
                     boolean executeQuery, boolean executeUpdate)
		throws SQLException
	{
		// need this additional check (it's also in the super.executeStatement
		// to ensure we have an activation for the getParams
		checkExecStatus();
		synchronized (getConnectionSynchronization())
		{
			wasNull = false;
			//Don't fetch the getParms into a local varibale
			//at this point because it is possible that the activation
			//associated with this callable statement may have become
			//stale. If the current activation is invalid, a new activation 
			//will be created for it in executeStatement call below. 
			//We should be using the ParameterValueSet associated with
			//the activation associated to the CallableStatement after
			//the executeStatement below. That ParameterValueSet is the
			//right object to hold the return value from the CallableStatement.
			try
			{
				getParms().validate();
			} catch (StandardException e)
			{
				throw EmbedResultSet.noStateChangeException(e);
			}

			/* KLUDGE - ? = CALL ... returns a ResultSet().  We
			 * need executeUpdate to be false in that case.
			 */
			boolean execResult = super.executeStatement(a, executeQuery,
				(executeUpdate && (! hasReturnOutputParameter)));

			//Fetch the getParms into a local variable now because the
			//activation associated with a CallableStatement at this 
			//point(after the executStatement) is the current activation. 
			//We can now safely stuff the return value of the 
			//CallableStatement into the following ParameterValueSet object.
			ParameterValueSet pvs = getParms();

			/*
			** If we have a return parameter, then we
			** consume it from the returned ResultSet
			** reset the ResultSet set to null.
			*/
			if (hasReturnOutputParameter)
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(results!=null, "null results even though we are supposed to have a return parameter");
				}
				boolean gotRow = results.next();
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(gotRow, "the return resultSet didn't have any rows");
				}

				try
				{
					DataValueDescriptor returnValue = pvs.getReturnValueForSet();
					returnValue.setValueFromResultSet(results, 1, true);
				} catch (StandardException e)
				{
					throw EmbedResultSet.noStateChangeException(e);
				}
				finally {
					results.close();
					results = null;
				}

				// This is a form of ? = CALL which current is not a procedure call.
				// Thus there cannot be any user result sets, so return false. execResult
				// is set to true since a result set was returned, for the return parameter.
				execResult = false;
			}
			return execResult;
		}
	}

	/*
	* CallableStatement interface
	* (the PreparedStatement part implemented by EmbedPreparedStatement)
	*/

	/**
	 * @see CallableStatement#registerOutParameter
	 * @exception SQLException NoOutputParameters thrown.
	 */
	public final void registerOutParameter(int parameterIndex, int sqlType)
		throws SQLException 
	{
		checkStatus();

		try {
			getParms().registerOutParameter(parameterIndex-1, sqlType, -1);
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#registerOutParameter
     * @exception SQLException NoOutputParameters thrown.
     */
    public final void registerOutParameter(int parameterIndex, int sqlType, int scale)
	    throws SQLException 
	{
		checkStatus();

		if (scale < 0)
			throw newSQLException(SQLState.BAD_SCALE_VALUE, new Integer(scale));
		try {
			getParms().registerOutParameter(parameterIndex-1, sqlType, scale);
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}


	/**
	 * JDBC 2.0
	 *
	 * Registers the designated output parameter
	 *
	 * @exception SQLException if a database-access error occurs.
	 */
 	public void registerOutParameter(int parameterIndex, int sqlType, 
 									 String typeName) 
 		 throws SQLException
 	{
 		throw Util.notImplemented("registerOutParameter");
 	}
 		 
 

    /**
	 * @see CallableStatement#wasNull
     * @exception SQLException NoOutputParameters thrown.
     */
    public boolean wasNull() throws SQLException 
	{
		checkStatus();
		return wasNull;
	}

    /**
	 * @see CallableStatement#getString
     * @exception SQLException NoOutputParameters thrown.
     */
    public String getString(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			String v =  getParms().getParameterForGet(parameterIndex-1).getString();
			wasNull = (v == null);
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getBoolean
     * @exception SQLException NoOutputParameters thrown.
     */
    public boolean getBoolean(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			boolean v = param.getBoolean();
			wasNull = (!v) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getByte
     * @exception SQLException NoOutputParameters thrown.
     */
    public byte getByte(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			byte b = param.getByte();
			wasNull = (b == 0) && param.isNull();
			return b;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getShort
     * @exception SQLException NoOutputParameters thrown.
     */
    public short getShort(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			short s = param.getShort();
			wasNull = (s == 0) && param.isNull();
			return s;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getInt
     * @exception SQLException NoOutputParameters thrown.
     */
    public int getInt(int parameterIndex) throws SQLException 
	{
		checkStatus();

		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			int v = param.getInt();
			wasNull = (v == 0) && param.isNull();
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getLong
     * @exception SQLException NoOutputParameters thrown.
     */
    public long getLong(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			long v = param.getLong();
			wasNull = (v == 0L) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

	/*
	** Methods using BigDecimal, moved back into EmbedCallableStatement
    ** because our small device implementation now requires CDC/FP 1.1.
	*/
    /**
     * JDBC 2.0
     *
     * Get the value of a NUMERIC parameter as a java.math.BigDecimal object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value (full precision); if the value is SQL NULL, 
     * the result is null 
     * @exception SQLException if a database-access error occurs.
     */
    public final BigDecimal getBigDecimal(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor dvd = getParms().getParameterForGet(parameterIndex-1);
			if (wasNull = dvd.isNull())
				return null;
			
			return com.splicemachine.db.iapi.types.SQLDecimal.getBigDecimal(dvd);
			
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getBigDecimal
     * @exception SQLException NoOutputParameters thrown.
     * @deprecated
     */
    public final BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
	{
    	BigDecimal v = getBigDecimal(parameterIndex);
    	if (v != null)
    		v = v.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
    	return v;
	}

    /**
	 * @see CallableStatement#getFloat
     * @exception SQLException NoOutputParameters thrown.
     */
    public float getFloat(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			float v = param.getFloat();
			wasNull = (v == 0.0) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}

    /**
	 * @see CallableStatement#getDouble
     * @exception SQLException NoOutputParameters thrown.
     */
    public double getDouble(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			DataValueDescriptor param = getParms().getParameterForGet(parameterIndex-1);
			double v = param.getDouble();
			wasNull = (v == 0.0) && param.isNull();
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getBytes
     * @exception SQLException NoOutputParameters thrown.
     */
    public byte[] getBytes(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			byte[] v =  getParms().getParameterForGet(parameterIndex-1).getBytes();
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getDate
     * @exception SQLException NoOutputParameters thrown.
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException
	{
		checkStatus();
		try {
            Date v = getParms().
                    getParameterForGet(parameterIndex-1).getDate(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getTime
     * @exception SQLException NoOutputParameters thrown.
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException
	{
		checkStatus();
		try {
            Time v = getParms().
                    getParameterForGet(parameterIndex-1).getTime(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}

	}

    /**
	 * @see CallableStatement#getTimestamp
     * @exception SQLException NoOutputParameters thrown.
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
	    throws SQLException 
	{
		checkStatus();
		try {
            Timestamp v = getParms().
                    getParameterForGet(parameterIndex-1).getTimestamp(cal);
			wasNull = (v == null);
			return v;
		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}
    /**
     * Get the value of a SQL DATE parameter as a java.sql.Date object
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
     * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Date getDate(int parameterIndex)
      throws SQLException 
	{
        return getDate(parameterIndex,getCal());
	}

    /**
     * Get the value of a SQL TIME parameter as a java.sql.Time object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
	 * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Time getTime(int parameterIndex)
      throws SQLException 
	{
        return getTime(parameterIndex,getCal());
	}

    /**
     * Get the value of a SQL TIMESTAMP parameter as a java.sql.Timestamp 
     * object.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return the parameter value; if the value is SQL NULL, the result is 
     * null
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Timestamp getTimestamp(int parameterIndex)
      throws SQLException 
	{
        return getTimestamp(parameterIndex,getCal());
	}

    /**
	 * @see CallableStatement#getObject
     * @exception SQLException NoOutputParameters thrown.
     */
	public final Object getObject(int parameterIndex) throws SQLException 
	{
		checkStatus();
		try {
			Object v = getParms().getParameterForGet(parameterIndex-1).getObject();
			wasNull = (v == null);
			return v;

		} catch (StandardException e)
		{
			throw EmbedResultSet.noStateChangeException(e);
		}
	}
	/**
	    * JDBC 3.0
	    *
	    * Retrieve the value of the designated JDBC DATALINK parameter as a java.net.URL object
	    *
	    * @param parameterIndex - the first parameter is 1, the second is 2
	    * @return a java.net.URL object that represents the JDBC DATALINK value used as
	    * the designated parameter
	    * @exception SQLException Feature not implemented for now.
		*/
		public URL getURL(int parameterIndex)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

		/**
	    * JDBC 3.0
	    *
	    * Sets the designated parameter to the given java.net.URL object. The driver
	    * converts this to an SQL DATALINK value when it sends it to the database.
	    *
	    * @param parameterName - the name of the parameter
	    * @param val - the parameter value
	    * @exception SQLException Feature not implemented for now.
		*/
		public void setURL(String parameterName, URL val)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

		/**
	    * JDBC 3.0
	    *
	    * Retrieves the value of a JDBC DATALINK parameter as a java.net.URL object
	    *
	    * @param parameterName - the name of the parameter
	    * @return the parameter value. If the value is SQL NULL, the result is null.
	    * @exception SQLException Feature not implemented for now.
		*/
		public URL getURL(String parameterName)
	    throws SQLException
		{
			throw Util.notImplemented();
		}

    /**
     * JDBC 2.0
     *
     * Get a BLOB OUT parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return an object representing a BLOB
     * @exception SQLException if a database-access error occurs.
     */
    public Blob getBlob (int parameterIndex) throws SQLException {
        Object o = getObject(parameterIndex);
        if (o == null || o instanceof Blob) {
            return (Blob) o;
        }
        throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
				Blob.class.getName(),
				Util.typeName(getParameterJDBCType(parameterIndex)));
    }

    /**
     * JDBC 2.0
     *
     * Get a CLOB OUT parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return an object representing a CLOB
     * @exception SQLException if a database-access error occurs.
     */
    public Clob getClob (int parameterIndex) throws SQLException {
        Object o = getObject(parameterIndex);
        if (o == null || o instanceof Clob) {
            return (Clob) o;
        }
        throw newSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
                Clob.class.getName(),
                Util.typeName(getParameterJDBCType(parameterIndex)));
    }
    
	public void addBatch() throws SQLException {

		checkStatus();
		ParameterValueSet pvs = getParms();

		int numberOfParameters = pvs.getParameterCount();

		for (int j=1; j<=numberOfParameters; j++) {

			switch (pvs.getParameterMode(j)) {
			case JDBC30Translation.PARAMETER_MODE_IN:
			case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
				break;
			case JDBC30Translation.PARAMETER_MODE_OUT:
			case JDBC30Translation.PARAMETER_MODE_IN_OUT:
				throw newSQLException(SQLState.OUTPUT_PARAMS_NOT_ALLOWED);
			}
		}

		super.addBatch();
	}


	public Object getObject(int parameterIndex,Map<String, Class<?>> map) throws SQLException{ throw new UnsupportedOperationException(); }
	public Ref getRef(int parameterIndex) throws SQLException{ throw new UnsupportedOperationException(); }
	public Array getArray(int parameterIndex) throws SQLException{ throw new UnsupportedOperationException(); }
	public void registerOutParameter(String parameterName,int sqlType) throws SQLException{ throw new UnsupportedOperationException();  }
	public void registerOutParameter(String parameterName,int sqlType,int scale) throws SQLException{ throw new UnsupportedOperationException();  }
	public void registerOutParameter(String parameterName,int sqlType,String typeName) throws SQLException{ throw new UnsupportedOperationException();  }
	public void setNull(String parameterName,int sqlType) throws SQLException{
		int index=findIndex(parameterName);
		setNull(index,sqlType);
	}

	public void setBoolean(String parameterName,boolean x) throws SQLException{
		setBoolean(findIndex(parameterName),x);
	}

	public void setByte(String parameterName,byte x) throws SQLException{
		setByte(findIndex(parameterName),x);
	}

	//@Override
	public void setShort(String parameterName,short x) throws SQLException{
		setShort(findIndex(parameterName),x);
	}

	//@Override
	public void setInt(String parameterName,int x) throws SQLException{
		setInt(findIndex(parameterName),x);
	}

	//@Override
	public void setLong(String parameterName,long x) throws SQLException{
		setLong(findIndex(parameterName),x);
	}

	//@Override
	public void setFloat(String parameterName,float x) throws SQLException{
		setFloat(findIndex(parameterName),x);
	}

	//@Override
	public void setDouble(String parameterName,double x) throws SQLException{
		setDouble(findIndex(parameterName),x);
	}

	//@Override
	public void setBigDecimal(String parameterName,BigDecimal x) throws SQLException{
		setBigDecimal(findIndex(parameterName),x);
	}

	//@Override
	public void setString(String parameterName,String x) throws SQLException{
		setString(findIndex(parameterName),x);
	}

	//@Override
	public void setBytes(String parameterName,byte[] x) throws SQLException{
		setBytes(findIndex(parameterName),x);
	}

	//@Override
	public void setDate(String parameterName,Date x) throws SQLException{
		setDate(findIndex(parameterName),x);
	}

	//@Override
	public void setTime(String parameterName,Time x) throws SQLException{
		setTime(findIndex(parameterName),x);
	}

	//@Override
	public void setTimestamp(String parameterName,Timestamp x) throws SQLException{
		setTimestamp(findIndex(parameterName),x);
	}

	//@Override
	public void setAsciiStream(String parameterName,InputStream x,int length) throws SQLException{
		setAsciiStream(findIndex(parameterName),x,length);
	}

	//@Override
	public void setBinaryStream(String parameterName,InputStream x,int length) throws SQLException{
		setBinaryStream(findIndex(parameterName),x,length);
	}

	//@Override
	public void setObject(String parameterName,Object x,int targetSqlType,int scale) throws SQLException{
		setObject(findIndex(parameterName),x,targetSqlType,scale);
	}

	//@Override
	public void setObject(String parameterName,Object x,int targetSqlType) throws SQLException{
		setObject(findIndex(parameterName),x,targetSqlType);
	}

	//@Override
	public void setObject(String parameterName,Object x) throws SQLException{
		setObject(findIndex(parameterName),x);
	}

	//@Override
	public void setCharacterStream(String parameterName,Reader reader,int length) throws SQLException{
		setCharacterStream(findIndex(parameterName),reader,length);
	}

	//@Override
	public void setDate(String parameterName,Date x,Calendar cal) throws SQLException{
		setDate(findIndex(parameterName),x,cal);
	}

	//@Override
	public void setTime(String parameterName,Time x,Calendar cal) throws SQLException{
		setTime(findIndex(parameterName),x,cal);
	}

	//@Override
	public void setTimestamp(String parameterName,Timestamp x,Calendar cal) throws SQLException{
		setTimestamp(findIndex(parameterName),x,cal);
	}

	//@Override
	public void setNull(String parameterName,int sqlType,String typeName) throws SQLException{
		setNull(findIndex(parameterName),sqlType,typeName);
	}

	//@Override
	public String getString(String parameterName) throws SQLException{
		return getString(findIndex(parameterName));
	}

	//@Override
	public boolean getBoolean(String parameterName) throws SQLException{
		return getBoolean(findIndex(parameterName));
	}

	//@Override
	public byte getByte(String parameterName) throws SQLException{
		return getByte(findIndex(parameterName));
	}

	//@Override
	public short getShort(String parameterName) throws SQLException{
		return getShort(findIndex(parameterName));
	}

	//@Override
	public int getInt(String parameterName) throws SQLException{
		return getInt(findIndex(parameterName));
	}

	//@Override
	public long getLong(String parameterName) throws SQLException{
		return getLong(findIndex(parameterName));
	}

	//@Override
	public float getFloat(String parameterName) throws SQLException{
		return getFloat(findIndex(parameterName));
	}

	//@Override
	public double getDouble(String parameterName) throws SQLException{
		return getDouble(findIndex(parameterName));
	}

	//@Override
	public byte[] getBytes(String parameterName) throws SQLException{
		return getBytes(findIndex(parameterName));
	}

	//@Override
	public Date getDate(String parameterName) throws SQLException{
		return getDate(findIndex(parameterName));
	}

	//@Override
	public Time getTime(String parameterName) throws SQLException{
		return getTime(findIndex(parameterName));
	}

	//@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException{
		return getTimestamp(findIndex(parameterName));
	}

	//@Override
	public Object getObject(String parameterName) throws SQLException{
		return getObject(findIndex(parameterName));
	}

	//@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException{
		return getBigDecimal(findIndex(parameterName));
	}

	//@Override
	public Object getObject(String parameterName,Map<String, Class<?>> map) throws SQLException{
		return getObject(findIndex(parameterName));
	}

	//@Override
	public Ref getRef(String parameterName) throws SQLException{
		return getRef(findIndex(parameterName));
	}

	//@Override
	public Blob getBlob(String parameterName) throws SQLException{
		return getBlob(findIndex(parameterName));
	}

	//@Override
	public Clob getClob(String parameterName) throws SQLException{
		return getClob(findIndex(parameterName));
	}

	//@Override
	public Array getArray(String parameterName) throws SQLException{
		return getArray(findIndex(parameterName));
	}

	//@Override
	public Date getDate(String parameterName,Calendar cal) throws SQLException{
		return getDate(findIndex(parameterName),cal);
	}

	//@Override
	public Time getTime(String parameterName,Calendar cal) throws SQLException{
		return getTime(findIndex(parameterName),cal);
	}

	//@Override
	public Timestamp getTimestamp(String parameterName,Calendar cal) throws SQLException{
		return getTimestamp(findIndex(parameterName),cal);
	}

	//@Override
	public RowId getRowId(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public RowId getRowId(String parameterName) throws SQLException{
		return getRowId(findIndex(parameterName));
	}

	//@Override
	public void setRowId(String parameterName,RowId x) throws SQLException{
		setRowId(findIndex(parameterName),x);
	}

	//@Override
	public void setNString(String parameterName,String value) throws SQLException{
		setNString(findIndex(parameterName),value);
	}

	//@Override
	public void setNCharacterStream(String parameterName,Reader value,long length) throws SQLException{
		setNCharacterStream(findIndex(parameterName),value,length);
	}

	//@Override
	public void setNClob(String parameterName,NClob value) throws SQLException{
		setNClob(findIndex(parameterName),value);
	}

	//@Override
	public void setClob(String parameterName,Reader reader,long length) throws SQLException{
		setClob(findIndex(parameterName),reader,length);
	}

	//@Override
	public void setBlob(String parameterName,InputStream inputStream,long length) throws SQLException{
		setBlob(findIndex(parameterName),inputStream,length);
	}

	//@Override
	public void setNClob(String parameterName,Reader reader,long length) throws SQLException{
		setNClob(findIndex(parameterName),reader,length);
	}

	//@Override
	public NClob getNClob(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public NClob getNClob(String parameterName) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setSQLXML(String parameterName,SQLXML xmlObject) throws SQLException{
		setSQLXML(findIndex(parameterName),xmlObject);
	}

	//@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException{
		return getSQLXML(findIndex(parameterName));
	}

	//@Override
	public String getNString(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public String getNString(String parameterName) throws SQLException{
		return getNString(findIndex(parameterName));
	}

	//@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException{
		return getNCharacterStream(findIndex(parameterName));
	}

	//@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public Reader getCharacterStream(String parameterName) throws SQLException{
		return getCharacterStream(findIndex(parameterName));
	}

	//@Override
	public void setBlob(String parameterName,Blob x) throws SQLException{
		setBlob(findIndex(parameterName),x);
	}

	//@Override
	public void setClob(String parameterName,Clob x) throws SQLException{
		setClob(findIndex(parameterName),x);
	}

	//@Override
	public void setAsciiStream(String parameterName,InputStream x,long length) throws SQLException{
		setAsciiStream(findIndex(parameterName),x,length);
	}

	//@Override
	public void setBinaryStream(String parameterName,InputStream x,long length) throws SQLException{
		setBinaryStream(findIndex(parameterName),x,length);
	}

	//@Override
	public void setCharacterStream(String parameterName,Reader reader,long length) throws SQLException{
		setCharacterStream(findIndex(parameterName),reader,length);
	}

	//@Override
	public void setAsciiStream(String parameterName,InputStream x) throws SQLException{
		setAsciiStream(findIndex(parameterName),x);
	}

	//@Override
	public void setBinaryStream(String parameterName,InputStream x) throws SQLException{
		setBinaryStream(findIndex(parameterName),x);
	}

	//@Override
	public void setCharacterStream(String parameterName,Reader reader) throws SQLException{
		setCharacterStream(findIndex(parameterName),reader);
	}

	//@Override
	public void setNCharacterStream(String parameterName,Reader value) throws SQLException{
		setNCharacterStream(findIndex(parameterName),value);
	}

	//@Override
	public void setClob(String parameterName,Reader reader) throws SQLException{
		setClob(findIndex(parameterName),reader);
	}

	//@Override
	public void setBlob(String parameterName,InputStream inputStream) throws SQLException{
		setBlob(findIndex(parameterName),inputStream);
	}

	//@Override
	public void setNClob(String parameterName,Reader reader) throws SQLException{
		setNClob(findIndex(parameterName),reader);
	}

	//@Override
	public <T> T getObject(int parameterIndex,Class<T> type) throws SQLException{
		Object object=getObject(parameterIndex);
		if(object==null) return null;
		return type.cast(object);
	}

	//@Override
	public <T> T getObject(String parameterName,Class<T> type) throws SQLException{
		return getObject(findIndex(parameterName),type);
	}

	//@Override
	public void setRef(int parameterIndex,Ref x) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setArray(int parameterIndex,Array x) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public ParameterMetaData getParameterMetaData() throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setRowId(int parameterIndex,RowId x) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNString(int parameterIndex,String value) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNCharacterStream(int parameterIndex,Reader value,long length) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNClob(int parameterIndex,NClob value) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNClob(int parameterIndex,Reader reader,long length) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setSQLXML(int parameterIndex,SQLXML xmlObject) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNCharacterStream(int parameterIndex,Reader value) throws SQLException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setNClob(int parameterIndex,Reader reader) throws SQLException{
		throw new UnsupportedOperationException();
	}

	protected int findIndex(String parameterName){
		throw new UnsupportedOperationException();
	}
}


