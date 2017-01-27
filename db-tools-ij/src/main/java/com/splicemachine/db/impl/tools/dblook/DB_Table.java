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

package com.splicemachine.db.impl.tools.dblook;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import com.splicemachine.db.tools.dblook;

public class DB_Table {

	// Prepared statements use throughout the DDL
	// generation process.
	private static PreparedStatement getColumnInfoStmt;
	private static PreparedStatement getColumnTypeStmt;
	private static PreparedStatement getAutoIncStmt;

	/* ************************************************
	 * Generate the DDL for all user tables in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @param tableIdToNameMap Mapping of table ids to table
	 *  names, for quicker reference.
	 * @return The DDL for the tables has been written
	 *  to output via Logs.java.
	 ****/

	public static void doTables(Connection conn, HashMap tableIdToNameMap)
		throws SQLException
	{

		// Prepare some statements for general use by this class.

		getColumnInfoStmt =
			conn.prepareStatement("SELECT C.COLUMNNAME, C.REFERENCEID, " +
			"C.COLUMNNUMBER FROM SYS.SYSCOLUMNS C, SYS.SYSTABLES T WHERE T.TABLEID = ? " +
			"AND T.TABLEID = C.REFERENCEID ORDER BY C.COLUMNNUMBER");

		getColumnTypeStmt = 
			conn.prepareStatement("SELECT COLUMNDATATYPE, COLUMNDEFAULT FROM SYS.SYSCOLUMNS " +
			"WHERE REFERENCEID = ? AND COLUMNNAME = ?");

		getAutoIncStmt = 
			conn.prepareStatement("SELECT AUTOINCREMENTSTART, " +
			"AUTOINCREMENTINC, COLUMNNAME, REFERENCEID, COLUMNDEFAULT FROM SYS.SYSCOLUMNS " +
			"WHERE COLUMNNAME = ? AND REFERENCEID = ?");

		// Walk through list of tables and generate the DDL for
		// each one.

		boolean firstTime = true;
		Set entries = tableIdToNameMap.entrySet();
		for (Iterator itr = entries.iterator(); itr.hasNext(); ) {

            Map.Entry entry = (Map.Entry)itr.next();
			String tableId = (String)entry.getKey();
			String tableName = (String)entry.getValue();
			if (dblook.isExcludedTable(tableName))
			// table isn't included in user-given list; skip it.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_TablesHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			Logs.writeToNewDDL("CREATE TABLE " + tableName + " (");

			// Get column list, and write DDL for each column.
			boolean firstCol = true;
			getColumnInfoStmt.setString(1, tableId);
			ResultSet columnRS = getColumnInfoStmt.executeQuery();
			while (columnRS.next()) {
				String colName = dblook.addQuotes(columnRS.getString(1));
				String createColString = createColumn(colName, columnRS.getString(2),
					columnRS.getInt(3));
				if (!firstCol)
					createColString = ", " + createColString;

				Logs.writeToNewDDL(createColString);
				firstCol = false;
			}

			columnRS.close();
			Logs.writeToNewDDL(")");
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		} // outer while.

		getColumnInfoStmt.close();
		getColumnTypeStmt.close();
		getAutoIncStmt.close();

	}

	/* ************************************************
	 * Generate the DDL for a specific column of the
	 * the table corresponding to the received tableId.
	 * @param colName the name of the column to generate.
	 * @param tableId Which table the column belongs to.
	 * @param colNum the number of the column to generate (1 =>
	 *  1st column, 2 => 2nd column, etc)
	 * @return The generated DDL, as a string.
	 ****/

	private static String createColumn(String colName, String tableId,
		int colNum) throws SQLException
	{

		getColumnTypeStmt.setString(1, tableId);
		getColumnTypeStmt.setString(2, dblook.stripQuotes(colName));

		ResultSet rs = getColumnTypeStmt.executeQuery();
		StringBuffer colDef = new StringBuffer();
		if (rs.next()) {

			colDef.append(dblook.addQuotes(dblook.expandDoubleQuotes(
				dblook.stripQuotes(colName))));
			colDef.append(" ");
			colDef.append(rs.getString(1));
			if (!reinstateAutoIncrement(colName, tableId, colDef) &&
						 rs.getString(2) != null) {

                String defaultText = rs.getString(2);

                if ( defaultText.startsWith( "GENERATED ALWAYS AS" ) )
                { colDef.append( " " ); }
				else { colDef.append(" DEFAULT "); }
                
				colDef.append( defaultText );
			}
		}

		rs.close();
		return colDef.toString();

	}

	/* ************************************************
	 * Generate autoincrement DDL for a given column and write it to
	 * received StringBuffer
	 * @param colName: Name of column that is autoincrement.
	 * @param tableId: Id of table in which column exists.
	 * @param colDef: StringBuffer to which DDL will be added.
	 * @return True if autoincrement DDL has been generated.
	 ****/

	public static boolean reinstateAutoIncrement(String colName,
		String tableId, StringBuffer colDef) throws SQLException
	{

		getAutoIncStmt.setString(1, dblook.stripQuotes(colName));
		getAutoIncStmt.setString(2, tableId);
		ResultSet autoIncCols = getAutoIncStmt.executeQuery();
		if (autoIncCols.next()) {

			long start = autoIncCols.getLong(1);
			if (!autoIncCols.wasNull()) {
				colDef.append(" GENERATED ");
				colDef.append(autoIncCols.getObject(5) == null ? 
					      "ALWAYS ":"BY DEFAULT ");
				colDef.append("AS IDENTITY (START WITH ");
				colDef.append(autoIncCols.getLong(1));
				colDef.append(", INCREMENT BY ");
				colDef.append(autoIncCols.getLong(2));
				colDef.append(")");
				return true;
			}
		}

		return false;

	}

}

