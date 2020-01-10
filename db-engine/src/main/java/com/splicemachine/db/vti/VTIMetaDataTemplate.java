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

import java.sql.SQLException;
import java.sql.ResultSetMetaData;

/**
	An abstract implementation of ResultSetMetaData (JDBC 1.2) that is useful
	when writing a VTI (virtual table interface).
	
	This class implements
	most of the methods of ResultSetMetaData, each one throwing a SQLException
	with the name of the method. A concrete subclass can then just implement
	the methods not implemented here and override any methods it needs
	to implement for correct functionality.
	<P>
	The methods not implemented here are
	<UL>
	<LI>getColumnCount()
	<LI>getColumnType()
	</UL>
	<BR>
	For virtual tables the database engine only calls methods defined
	in the JDBC 1.2 definition of java.sql.ResultSetMetaData.
	<BR>
	Classes that implement a JDBC 2.0 conformant java.sql.ResultSetMetaData can be used
	as the meta data for virtual tables.
	<BR>
	Developers can use the VTIMetaDataTemplate20 instead of this class when
	developing in a Java 2 environment.
 */

public abstract class VTIMetaDataTemplate implements ResultSetMetaData {

    /**
     * Is the column automatically numbered, and thus read-only?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if the column is automatically numbered
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLException("isAutoIncrement");
	}


    /**
     * Does a column's case matter?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if the column is case-sensitive
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLException("isCaseSensitive");
	}
	

    /**
     * Can the column be used in a WHERE clause?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if the column is searchable
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isSearchable(int column) throws SQLException{
		throw new SQLException("isSearchable");
	}


    /**
     * Is the column a cash value?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if the column is a cash value
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isCurrency(int column) throws SQLException{
		throw new SQLException("isCurrency");
	}


    /**
     * Can you put a NULL in this column?		
     *
     * @param column the first column is 1, the second is 2, ...
     * @return columnNoNulls, columnNullable or columnNullableUnknown
     * @exception SQLException if a database-access error occurs.
     */
	public int isNullable(int column) throws SQLException{
		throw new SQLException("isNullable");
	}


    /**
     * Is the column a signed number?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if the column is a signed number
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isSigned(int column) throws SQLException {
		throw new SQLException("isSigned");
	}


    /**
     * What's the column's normal maximum width in chars?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's maximum width
     * @exception SQLException if a database-access error occurs.
     */
	public  int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLException("getColumnDisplaySize");
	}


    /**
     * What's the suggested column title for use in printouts and
     * displays?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's title
     * @exception SQLException if a database-access error occurs.
     */
	public String getColumnLabel(int column) throws SQLException {
		throw new SQLException("getColumnLabel");
	}
	

    /**
     * What's a column's name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @exception SQLException if a database-access error occurs.
     */
	public String getColumnName(int column) throws SQLException {
		throw new SQLException("getColumnName");
	}


    /**
     * What's a column's table's schema?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception SQLException if a database-access error occurs.
     */
	public  String getSchemaName(int column) throws SQLException {
		throw new SQLException("getSchemaName");
	}


    /**
     * How many decimal digits are in the column?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's precision
     * @exception SQLException if a database-access error occurs.
     */
	public int getPrecision(int column) throws SQLException {
		throw new SQLException("getPrecision");
	}


    /**
     * What's a column's number of digits to the right of the decimal point?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's scale
     * @exception SQLException if a database-access error occurs.
     */
	public  int getScale(int column) throws SQLException {
		throw new SQLException("getScale");
	}
	

    /**
     * What's a column's table name? 
     *
	 * @param column the first column is 1, the second is 2, ...
     * @return the column's table name or "" if not applicable
     * @exception SQLException if a database-access error occurs.
     */
	public  String getTableName(int column) throws SQLException {
		throw new SQLException("getTableName");
	}


    /**
     * What's a column's table's catalog name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's table's catalog name or "" if not applicable.
     * @exception SQLException if a database-access error occurs.
     */
	public String getCatalogName(int column) throws SQLException {
		throw new SQLException("getCatalogName");
	}


    /**
     * What's a column's data source specific type name?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the column's type name
     * @exception SQLException if a database-access error occurs.
     */
	public  String getColumnTypeName(int column) throws SQLException {
		throw new SQLException("getColumnTypeName");
	}


    /**
     * Is a column definitely not writable?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true - vti's are read only
	 *         false - column is not read-only
     * @exception SQLException if a database-access error occurs.
     */
	public  boolean isReadOnly(int column) throws SQLException {
		return true;
	}


    /**
     * Is it possible for a write on the column to succeed?
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if column is possibly writable
     * @exception SQLException if a database-access error occurs.
     */
	public  boolean isWritable(int column) throws SQLException {
		return false;
	}

    /**
     * Will a write on the column definitely succeed?	
     *
     * @param column the first column is 1, the second is 2, ...
     * @return true if column is definitely writable
     * @exception SQLException if a database-access error occurs.
     */
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	/*
	** JDBC 2.0
	*/

	/**
	 * Returns the fully-qualified name of the Java class whose instances
	 * are manufactured if the method <code>ResultSet.<!-- -->getObject</code>
	 * is called to retrieve a value from the column. JDBC 2.0.
	 *
	 * @exception SQLException if a database-access error occurs
	 */
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLException("getColumnClassName");
	}

//	@Override
	public int getColumnCount() throws SQLException{ throw new UnsupportedOperationException(); }
//	@Override
	public int getColumnType(int column) throws SQLException{ throw new UnsupportedOperationException(); }

//	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException{ throw new UnsupportedOperationException(); }

//	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException{ throw new UnsupportedOperationException(); }
}
