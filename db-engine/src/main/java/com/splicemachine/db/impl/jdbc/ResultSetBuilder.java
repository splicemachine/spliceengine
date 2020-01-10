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

package com.splicemachine.db.impl.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBit;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLClob;
import com.splicemachine.db.iapi.types.SQLDate;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongVarbit;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLLongvarchar;
import com.splicemachine.db.iapi.types.SQLReal;
import com.splicemachine.db.iapi.types.SQLSmallint;
import com.splicemachine.db.iapi.types.SQLTime;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLTinyint;
import com.splicemachine.db.iapi.types.SQLVarbit;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

/**
 * Builder class to make it simpler to define and create a ResultSet.  Creating a Derby ResultSet can include creating all sorts
 * of objects (ExecRows, DataValueDescriptors, GenericColumnDescriptors, etc.).  And you end up copying and pasting many lines of
 * boilerplate code to create a simple result set.  This builder does it all for you.  All you need to do is define the columns and
 * add the rows.
 * 
 * <h3>Example:</h3>
 * <pre>
 * 		// Step 1: Define the columns.
 *		ResultSetBuilder rsBuilder = new ResultSetBuilder();
 *		rsBuilder.getColumnBuilder()
 *			.addColumn("PROCEDURE_CAT", Types.VARCHAR, 128)
 *			.addColumn("COLUMN_TYPE", Types.SMALLINT)
 *
 *
 *
 *			.addColumn("DATA_TYPE", Types.INTEGER)
 *		;
 *		// Step 2: Add the rows.  NOTE: The columns are now frozen (no changes allowed).
 *		RowBuilder rowBuilder = rsBuilder.getRowBuilder();
 *		rowBuilder.getDvd(0).setValue("Utilities");
 *		rowBuilder.getDvd(1).setValue(24);
 *		rowBuilder.getDvd(2).setValue(777);
 *		rowBuilder.addRow();
 *		rowBuilder.getDvd("PROCEDURE_CAT").setValue("Admin");
 *		rowBuilder.getDvd("COLUMN_TYPE").setValue(42);
 *		rowBuilder.getDvd("DATA_TYPE").setValue(6502);
 *		rowBuilder.addRow();
 *		// Step 3: Build the result set and return it.
 *		ResultSet rs = rsBuilder.buildResultSet(embedConn);
 * </pre>
 *
 * @author dwinters
 */
public class ResultSetBuilder {

	private ColumnBuilder columnBuilder = null;
	private RowBuilder rowBuilder = null;
	private ResultSet resultSet = null;
	private boolean freezeCols = false;
	private int numCols = 0;  // Once the columns are frozen, store the number of them.

	/**
	 * Empty constructor
	 */
	public ResultSetBuilder() {
	}

	/**
	 * Class to define the columns in the result set.  This is required before adding rows.  Once the RowBuilder has been
	 * instantiated (fetched), columns are frozen and cannot be modified for the result set.
	 */
	public class ColumnBuilder {

		// Column-related data structures
		private List<ResultColumnDescriptor> columnInfoList = null;
		private List<DataValueDescriptor> dvdsList = null;  // The DVDs are defined as the columns are defined even though they define the ExecRow structure.

		/**
		 * Private constructor to limit instantiation to the parent builder class.
		 */
		private ColumnBuilder() {
			columnInfoList = new ArrayList<>();
			dvdsList = new ArrayList<>();
		}

		/**
		 * Add a new column for the result set to be built.
		 * @param name name of column
		 * @param jdbcTypeId JDBC type ID
		 * @param length length of data type, usually this is used for VARCHAR and CHAR columns
		 * @return column builder
		 * @throws StandardException
		 */
		public ColumnBuilder addColumn(String name, int jdbcTypeId, int length) throws StandardException {
			if (freezeCols) {
				throw StandardException.newException("CANNOT_ADD_COLUMNS_TO_FROZEN_COLUMN_BUILDER");
			}
			columnInfoList.add(new GenericColumnDescriptor(name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcTypeId, length)));
			addRowDvd(jdbcTypeId);
			return this;
		}

		/**
		 * Add a new column for the result set to be built.
		 * @param name name of column
		 * @param jdbcTypeId JDBC type ID
		 * @return column builder
		 * @throws StandardException
		 */
		public ColumnBuilder addColumn(String name, int jdbcTypeId) throws StandardException {
			if (freezeCols) {
				throw StandardException.newException("CANNOT_ADD_COLUMNS_TO_FROZEN_COLUMN_BUILDER");
			}
			columnInfoList.add(new GenericColumnDescriptor(name, DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcTypeId)));
			addRowDvd(jdbcTypeId);
			return this;
		}

		/**
		 * Worker method to add the corresponding DVD object for the specified JDBC type.
		 * @param jdbcTypeId
		 * @throws StandardException
		 */
		private void addRowDvd(int jdbcTypeId) throws StandardException {
			switch (jdbcTypeId) {
			case Types.TINYINT:
				dvdsList.add(new SQLTinyint());
				break;
			case Types.SMALLINT:
				dvdsList.add(new SQLSmallint());
				break;
			case Types.INTEGER:
				dvdsList.add(new SQLInteger());
				break;
			case Types.BIGINT:
				dvdsList.add(new SQLLongint());
				break;
			case Types.FLOAT:
				dvdsList.add(new SQLReal());
				break;
			case Types.REAL:
				dvdsList.add(new SQLReal());
				break;
			case Types.DOUBLE:
				dvdsList.add(new SQLDouble());
				break;
			case Types.DECIMAL:
				dvdsList.add(new SQLDecimal());
				break;
			case Types.NUMERIC:
				dvdsList.add(new SQLDecimal());
				break;
			case Types.CHAR:
				dvdsList.add(new SQLChar());
				break;
			case Types.VARCHAR:
				dvdsList.add(new SQLVarchar());
				break;
			case Types.DATE:
				dvdsList.add(new SQLDate());
				break;
			case Types.TIME:
				dvdsList.add(new SQLTime());
				break;
			case Types.TIMESTAMP:
				dvdsList.add(new SQLTimestamp());
				break;
			case Types.BIT:
				dvdsList.add(new SQLBit());
				break;
			case Types.BOOLEAN:
				dvdsList.add(new SQLBoolean());
				break;
			case Types.BINARY:
				dvdsList.add(new SQLBit());
				break;
			case Types.VARBINARY:
				dvdsList.add(new SQLVarbit());
				break;
			case Types.LONGVARBINARY:
				dvdsList.add(new SQLLongVarbit());
				break;
			case Types.LONGVARCHAR:
				dvdsList.add(new SQLLongvarchar());
				break;
			case Types.BLOB:
				dvdsList.add(new SQLBlob());
				break;
			case Types.CLOB:
				dvdsList.add(new SQLClob());
				break;
			case JDBC40Translation.SQLXML:
				dvdsList.add(new SQLLongvarchar());
				break;
			default:
				throw StandardException.newException(SQLState.NET_INVALID_JDBC_TYPE_FOR_PARAM, jdbcTypeId);
			}
		}
	}

	/**
	 * Initialize and return the column builder for defining the columns of the result set.
	 * @return column builder
	 */
	public ColumnBuilder getColumnBuilder() {
		if (columnBuilder == null) {
			columnBuilder = new ColumnBuilder();
		}
		return columnBuilder;
	}

	/**
	 * Class to add rows to the result set.
	 * Once this class has been initialized, columns can no longer be changed for the result set.
	 */
	public class RowBuilder {

		// Row-related data structures
		private ArrayList<ExecRow> rowsList = null;
		private DataValueDescriptor[] dvds = null;
		private ExecRow dataTemplate = null;

		/**
		 * Private constructor to limit instantiation to the parent builder class.
		 */
		private RowBuilder() {
			rowsList = new ArrayList<>();
			dvds = columnBuilder.dvdsList.toArray(new DataValueDescriptor[numCols]);
			dataTemplate = new ValueRow(numCols);
			dataTemplate.setRowArray(dvds);
		}

		/**
		 * Instead of implementing ~20 methods to set all of the various data types, just use the DVDs directly to set the values
		 * of the rows.  It breaks encapsulation, but it sure is less typing.  ;-p
		 * @param columnIndex
		 * @return DVD at the index position
		 */
		public DataValueDescriptor getDvd(int columnIndex) {
			return dvds[columnIndex];
		}

		/**
		 * Instead of implementing ~20 methods to set all of the various data types, just use the DVDs directly to set the values
		 * of the rows.  It breaks encapsulation, but it sure is less typing.  ;-p
		 * @param columnName
		 * @return DVD for the named column
		 */
		public DataValueDescriptor getDvd(String columnName) throws StandardException {
			return getDvd(findColumn(columnName));
		}

		/**
		 * Add a row and prepare to add another if desired.
		 * @return the row builder
		 */
		public RowBuilder addRow() {
			rowsList.add(dataTemplate.getClone());
			resetDvds();
			return rowBuilder;
		}
	}

	/**
	 * Initialize and return the row builder for adding rows to the result set.
	 * Once this method has been called, columns can no longer be changed for the result set.
	 * @return
	 */
	public RowBuilder getRowBuilder() {

		if (rowBuilder == null) {
			// Just mark the columns as frozen if they haven't been marked yet...
			if (!isFrozen()) {
				freezeColumns();
			}
			rowBuilder = new RowBuilder();
		}
		return rowBuilder;
	}

	/**
	 * Check whether the definitions of the columns have been frozen.
	 * @return true = frozen columns, false = unfrozen columns
	 */
	public boolean isFrozen() {
		return freezeCols;
	}

	/**
	 * Freeze the definition of the columns.
	 */
	public void freezeColumns() {
		freezeCols = true;
		numCols = columnBuilder.dvdsList.size();
	}

	/**
	 * Return the index of the named column.
	 * @param columnName
	 * @return index of the named column
	 * @throws StandardException
	 */
	public int findColumn(String columnName) throws StandardException {
		for (int i = 0; i < columnBuilder.columnInfoList.size(); i++) {
			if (columnName != null && columnName.equals(columnBuilder.columnInfoList.get(i).getName())) {
				return i+1;
			}
		}
		throw StandardException.newException("Unknown column name");
	}

	/**
	 * Set all the DVDs to SQL NULL, so they can be reused for the next row to be added.
	 */
	private void resetDvds() {
		for (DataValueDescriptor dvd : columnBuilder.dvdsList) {
			dvd.setToNull();
		}
		for (DataValueDescriptor dvd : rowBuilder.dvds) {
			dvd.setToNull();
		}
	}

	/**
	 * Return the generated result set.  After the columns have been defined and the rows added, this method returns the result set.
	 * @param conn
	 * @return generated result set
	 * @throws StandardException
	 * @throws SQLException
	 */
	public ResultSet buildResultSet(EmbedConnection conn) throws StandardException, SQLException {
		if (resultSet == null) {
			// Just mark the columns as frozen if they haven't been marked yet...
			if (!isFrozen()) {
				freezeColumns();
			}
			if (rowBuilder == null) {
				getRowBuilder(); // forces creation so empty results set can be created instead of NPE
			}
			IteratorNoPutResultSet resultsToWrap =
				new IteratorNoPutResultSet(
					rowBuilder.rowsList,
					columnBuilder.columnInfoList.toArray(new ResultColumnDescriptor[numCols]),
					conn.getLanguageConnection().getLastActivation());
			resultsToWrap.openCore();
			resultSet = new EmbedResultSet40(conn, resultsToWrap, false, null, true);
		}
		return resultSet;
	}
}
