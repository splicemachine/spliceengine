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

public class DB_Check {

	/* ************************************************
	 * Generate the DDL for all checks in a given
	 * database.
	 * @param conn Connection to the source database.
	 * @return The DDL for the indexes has been written
	 *  to output via Logs.java.
	 ****/

	public static void doChecks(Connection conn)
		throws SQLException
	{

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT CS.CONSTRAINTNAME, " +
			"CS.TABLEID, CS.SCHEMAID, CK.CHECKDEFINITION FROM SYS.SYSCONSTRAINTS CS, " +
			"SYS.SYSCHECKS CK WHERE CS.CONSTRAINTID = " +
			"CK.CONSTRAINTID AND CS.STATE != 'D' ORDER BY CS.TABLEID");

		boolean firstTime = true;
		while (rs.next()) {

			String tableId = rs.getString(2);
			String tableName = dblook.lookupTableId(tableId);
			if (dblook.isExcludedTable(tableName))
			// table isn't specified in user-given list; skip it.
				continue;

			if (firstTime) {
				Logs.reportString("----------------------------------------------");
				Logs.reportMessage("DBLOOK_ChecksHeader");
				Logs.reportString("----------------------------------------------\n");
			}

			StringBuffer chkString = createCheckString(tableName, rs);
			Logs.writeToNewDDL(chkString.toString());
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;

		}

		stmt.close();
		rs.close();

	}

	/* ************************************************
	 * Generate DDL for a specific check.
	 * @param tableName Name of the table on which the check
	 *   exists.
	 * @param aCheck Information about the check in question.
	 * @return The DDL for the specified check has been
	 *  generated returned as a StringBuffer.
	 ****/

	private static StringBuffer createCheckString (String tableName,
		ResultSet aCheck) throws SQLException
	{

		StringBuffer sb = new StringBuffer ("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" ADD");

		String constraintName = dblook.addQuotes(
			dblook.expandDoubleQuotes(aCheck.getString(1)));
		sb.append(" CONSTRAINT ");
		sb.append(constraintName);
		sb.append(" CHECK ");
		sb.append(dblook.removeNewlines(aCheck.getString(4)));

		return sb;

	}

}
