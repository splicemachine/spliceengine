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

package com.splicemachine.db.impl.tools.dblook;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.db.tools.dblook;

public class DB_Index {

	/* ************************************************
	 * Generate the DDL for all indexes in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the indexes has been written
	 *  to output via Logs.java.
	 ****/

	public static void doIndexes(Connection conn)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT TABLEID, CONGLOMERATENAME, " +
			"DESCRIPTOR, SCHEMAID, ISINDEX, ISCONSTRAINT FROM SYS.SYSCONGLOMERATES " +
			"ORDER BY TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			if (!rs.getBoolean(5) ||	// (isindex == false)
				rs.getBoolean(6))		// (isconstraint == true)
			// then skip it.
				continue;

			String tableId = rs.getString(1);
			String tableName = dblook.lookupTableId(tableId);
			if (tableName == null)
			// then tableId isn't a user table, so we can skip it.
				continue;
			else if (dblook.isExcludedTable(tableName))
			// table isn't specified in user-given list.
				continue;

			String iSchema = dblook.lookupSchemaId(rs.getString(4));
			if (dblook.isIgnorableSchema(iSchema))
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_IndexesHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			String iName = dblook.addQuotes(
				dblook.expandDoubleQuotes(rs.getString(2)));
			iName = iSchema + "." + iName;

			StringBuffer createIndexString = createIndex(iName, tableName,
				tableId, rs.getString(3));

			Logs.writeToNewDDL(createIndexString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		rs.close();
		stmt.close();

	}

	/* ************************************************
	 * Generate DDL for a specific index.
	 * @param ixName Name of the index.
	 * @param tableName Name of table on which the index exists.
	 * @param tableId Id of table on which the index exists.
	 * @param ixDescribe Column list for the index.
	 * @return The DDL for the specified index, as a string
	 *  buffer.
	 ****/

	private static StringBuffer createIndex(String ixName, String tableName,
		String tableId, String ixDescribe) throws SQLException
	{

		StringBuffer sb = new StringBuffer("CREATE ");
		if (ixDescribe.contains("UNIQUE"))
			sb.append("UNIQUE ");

		// Note: We leave the keyword "BTREE" out since it's not
		// required, and since it is not recognized by DB2.

		sb.append("INDEX ");
		sb.append(ixName);
		sb.append(" ON ");
		sb.append(tableName);
		sb.append(" (");
		sb.append(dblook.getColumnListFromDescription(tableId, ixDescribe));
		sb.append(")");
		return sb;

	}

}
